#ifdef WITH_POSTGRESQL

#include "PGFetch.hpp"
#include "apostol/application.hpp"

#include "apostol/pg_utils.hpp"

#include <fmt/format.h>

#include <algorithm>

namespace apostol
{

namespace
{

// The shortest resend age over the fetch timeout: the flight itself is bounded
// by timeout + 10 s (the task deadline), and the age of a row counts from its
// INSERT, not its COMMIT — 20 s more for the producer's transaction.
constexpr long sweep_age_margin_s = 10 + 20;

constexpr auto probe_retry  = std::chrono::seconds(5);
constexpr auto sweep_retry  = std::chrono::seconds(10);

// "…" with every " doubled. Local on purpose: pq_quote_ident arrived in
// libapostol only with a569f64 (T587), and this module builds against older
// pins in other projects.
std::string quote_ident(std::string_view name)
{
    std::string out;
    out.reserve(name.size() + 2);
    out += '"';
    for (char c : name) {
        if (c == '"')
            out += '"';
        out += c;
    }
    out += '"';
    return out;
}

// Any bytes as a bytea expression: a NUL or invalid UTF-8 in a response body
// would break a text literal and, with it, storing the response.
std::string bytea_sql(std::string_view bytes)
{
    static constexpr char hex[] = "0123456789abcdef";
    std::string out = "decode('";
    out.reserve(bytes.size() * 2 + 16);
    for (unsigned char c : bytes) {
        out += hex[c >> 4];
        out += hex[c & 0x0f];
    }
    out += "', 'hex')";
    return out;
}

std::string fail_sql(const std::string& id, const std::string& message)
{
    return fmt::format("SELECT http.fail({}, {})", pq_quote_literal(id), pq_quote_literal(message));
}

} // namespace

// ─── Construction ───────────────────────────────────────────────────────────

PGFetch::PGFetch(Application& app, EventLoop& loop)
    : pool_(app.db_pool())
    , log_(app.logger())
    , modules_(app.module_manager())
    , fetch_(loop)
    , timeout_ms_(0)
    , sweep_age_s_(90)
    , sweep_limit_(100)
    , sweep_possible_(false)
    , enabled_(true)
{
    if (auto* cfg = app.module_config("PGFetch")) {
        if (cfg->contains("timeout") && (*cfg)["timeout"].is_number())
            timeout_ms_ = (*cfg)["timeout"].get<long>() * 1000;
        if (cfg->contains("sweep_age") && (*cfg)["sweep_age"].is_number())
            sweep_age_s_ = (*cfg)["sweep_age"].get<long>();
        if (cfg->contains("sweep_limit") && (*cfg)["sweep_limit"].is_number())
            sweep_limit_ = (*cfg)["sweep_limit"].get<long>();
    }
    if (timeout_ms_ > 0)
        fetch_.set_timeout(timeout_ms_);

    if (sweep_limit_ < 1)
        sweep_limit_ = 100;

    // A request is resent only when nothing can still be carrying it: that
    // needs the moment LISTEN is up (on_ready) and a bounded flight (timeout).
#ifdef APOSTOL_PG_LISTEN_READY
    if (timeout_ms_ > 0) {
        sweep_possible_ = true;
        const long floor_s = timeout_ms_ / 1000 + sweep_age_margin_s;
        if (sweep_age_s_ < floor_s) {
            log_.warn("PGFetch: sweep_age {} s is shorter than timeout + {} s — raised to {} s",
                      sweep_age_s_, sweep_age_margin_s, floor_s);
            sweep_age_s_ = floor_s;
        }
    } else {
        log_.warn("PGFetch: no timeout — a request in flight is unbounded, "
                  "requests whose NOTIFY was lost are not resent");
    }
#else
    log_.warn("PGFetch: libapostol without PgPool listen-ready (APOSTOL_PG_LISTEN_READY) — "
              "requests whose NOTIFY was lost are not resent");
#endif
}

// ─── Lifecycle ──────────────────────────────────────────────────────────────

void PGFetch::on_start()
{
    probe();

    auto on_notify_cb = [this](std::string_view ch, std::string_view payload) {
        on_notify(ch, payload);
    };
#ifdef APOSTOL_PG_LISTEN_READY
    pool_.listen("http", std::move(on_notify_cb),
                 [this](std::string_view /*channel*/) { on_listen_ready(); });
#else
    pool_.listen("http", std::move(on_notify_cb));
#endif
}

// A request out on the wire has no outcome yet, and nothing waits for one:
// the shutdown drain counts database queries only, so the process would exit
// with the row in state 1 — never sent again without a lifetime, and with one
// sent twice (T672). It is failed here, as a timeout is: the remote may have
// acted on it, and the message says so. A failure written from on_stop() is
// delivered by the drain that follows; an answer arriving after it is
// dropped (timed_out). A row read once the shutdown has begun is not sent
// (do_query): it stays as the database has it — resent by http.sweep on the
// next start if it has a lifetime, like a request that never reached us.

void PGFetch::on_stop()
{
    pool_.unlisten("http");

    std::size_t failed = 0;
    for (auto& task : queue_) {
        if (task->sent && !task->settling && !task->timed_out) {
            task->timed_out = true;
            do_fail(task, "stopped with the request in flight: the remote may have received it, outcome unknown");
            ++failed;
        }
    }
    if (failed > 0)
        log_.warn("PGFetch: stopping with {} request(s) in flight — marked failed, outcome unknown", failed);
}

// ─── probe ──────────────────────────────────────────────────────────────────
//
// The database decides what is sent (http.take) and what is resent
// (http.sweep). A database without them — an older db-platform — gets the
// old reading by id and no resend: the module runs in projects on other pins.

void PGFetch::probe()
{
    probing_ = true;

    pool_.execute(
        "SELECT to_regprocedure('http.take(uuid)') IS NOT NULL"
        " AND to_regprocedure('http.sweep(interval,integer)') IS NOT NULL",
        [this](std::vector<PgResult> results) {
            probing_ = false;

            const bool has = !results.empty() && results[0].rows() > 0 &&
                             results[0].columns() > 0 && results[0].value(0, 0) != nullptr &&
                             results[0].value(0, 0)[0] == 't';
            const Source next = has ? Source::take : Source::request;

            if (next == source_)
                return;
            source_ = next;

            if (has)
                log_.notice("PGFetch: requests read through http.take; {}",
                            sweep_possible_
                                ? fmt::format("lost NOTIFY resent by http.sweep {} s after LISTEN is up",
                                              sweep_age_s_)
                                : std::string("no resend (see above)"));
            else
                log_.warn("PGFetch: http.take/http.sweep not in the database — requests read by "
                          "http.request(id), requests whose NOTIFY was lost are not resent");
        },
        [this](std::string_view error) {
            probing_ = false;
            next_probe_ = std::chrono::steady_clock::now() + probe_retry;
            // A re-probe (on_listen_ready) of a source already known changes
            // nothing: sending goes on the way it was.
            if (source_ == Source::unknown)
                log_.error("PGFetch: probe for http.take/http.sweep failed ({} request(s) waiting, none sent until "
                           "it answers): {}", queue_.size(), error);
            else
                log_.error("PGFetch: re-probe for http.take/http.sweep failed, still reading through {}: {}",
                           source_ == Source::take ? "http.take" : "http.request", error);
        });
}

// ─── on_notify / on_listen_ready / enqueue ──────────────────────────────────

void PGFetch::on_notify(std::string_view /*channel*/, std::string_view payload)
{
    // Payload is the request_id (UUID string)
    if (payload.empty())
        return;

    enqueue(std::string(payload));
}

void PGFetch::on_listen_ready()
{
    // Every (re)subscription: whatever was inserted before it may have
    // notified nobody. Once it is older than any flight, sweep hands it out.
    if (!probing_)
        probe();

    if (!sweep_possible_)
        return;

    // Keep an earlier due sweep: a listener that flaps faster than sweep_age
    // must not push the sweep back forever. What this ready adds is picked up
    // when that sweep completes (see do_sweep).
    const auto now = std::chrono::steady_clock::now();
    ready_at_ = now;
    if (!sweep_due_)
        sweep_due_ = now + std::chrono::seconds(sweep_age_s_);
}

void PGFetch::enqueue(std::string id)
{
    // One task per id: NOTIFY and sweep may both name it, and a task stays in
    // queue_ until remove_task — waiting or in flight, one check covers both.
    for (const auto& t : queue_) {
        if (t->id == id)
            return;
    }

    auto task = std::make_shared<FetchTask>();
    task->id = std::move(id);

    queue_.push_back(std::move(task));
}

// ─── heartbeat ──────────────────────────────────────────────────────────────

void PGFetch::heartbeat(std::chrono::system_clock::time_point /*now*/)
{
    auto now_steady = std::chrono::steady_clock::now();

    if (source_ == Source::unknown && !probing_ && now_steady >= next_probe_)
        probe();

    process_queue();
    check_timeouts(now_steady);

    if (sweep_due_ && !sweeping_ && now_steady >= *sweep_due_) {
        if (source_ == Source::take)
            do_sweep();
        else if (source_ == Source::request)
            sweep_due_.reset();
        // Source::unknown — wait for the probe
    }
}

// ─── do_sweep ───────────────────────────────────────────────────────────────

void PGFetch::do_sweep()
{
    sweeping_ = true;
    sweep_started_ = std::chrono::steady_clock::now();

    auto sql = fmt::format(
        "SELECT s::text FROM http.sweep(make_interval(secs => {}), {}) AS s",
        sweep_age_s_, sweep_limit_);

    pool_.execute(sql,
        [this](std::vector<PgResult> results) {
            sweeping_ = false;

            int rows = 0;
            if (!results.empty() && results[0].columns() > 0) {
                rows = results[0].rows();
                for (int i = 0; i < rows; ++i) {
                    const char* id = results[0].value(i, 0);
                    if (id && id[0] != '\0')
                        enqueue(id);
                }
            }

            if (rows > 0)
                log_.notice("PGFetch: http.sweep handed out {} request(s) to resend", rows);

            const auto age = std::chrono::seconds(sweep_age_s_);

            // A full batch — there may be more: again on the next heartbeat.
            // Otherwise done, unless a (re)subscription came too late for this
            // sweep to cover it: its rows were younger than sweep_age.
            if (rows >= sweep_limit_)
                sweep_due_ = std::chrono::steady_clock::now();
            else if (ready_at_ && *ready_at_ + age > sweep_started_)
                sweep_due_ = *ready_at_ + age;
            else
                sweep_due_.reset();
        },
        [this](std::string_view error) {
            sweeping_ = false;
            // Not earlier than the last ready + sweep_age: before that its rows
            // are too young to be handed out and the retry would miss them.
            auto due = std::chrono::steady_clock::now() + sweep_retry;
            if (ready_at_)
                due = std::max(due, *ready_at_ + std::chrono::seconds(sweep_age_s_));
            sweep_due_ = due;
            log_.error("PGFetch: http.sweep failed: {}", error);
        });
}

// ─── process_queue ──────────────────────────────────────────────────────────

void PGFetch::process_queue()
{
    // Until the probe answers it is not known how a row is read. The queue
    // waits, with no deadline, however long the probe keeps failing or goes
    // unanswered (a failure line counts what waits; a database that is away
    // leaves no line of ours — nothing arrives then either). Deliberately:
    // reading by http.request instead would send what http.take would close
    // as expired — a lifetime is the point of T577 — and failing a waiting
    // request needs the very database that is answering with errors. When
    // the probe answers, each waiting row is read as that database reads
    // it: http.take settles it by its lifetime; a database without
    // http.take has no lifetime to break.
    if (source_ == Source::unknown)
        return;

    for (auto& task : queue_) {
        if (!task->in_progress) {
            task->in_progress = true;
            // Deadline from dispatch, not from enqueue: a task may wait for the
            // probe. Timeout + 10s grace (mirrors v1 pattern).
            long effective_timeout = timeout_ms_ > 0 ? timeout_ms_ + 10000 : 60000;
            task->deadline = std::chrono::steady_clock::now()
                           + std::chrono::milliseconds(effective_timeout);
            do_query(task);
        }
    }
}

// ─── do_query ───────────────────────────────────────────────────────────────

void PGFetch::do_query(std::shared_ptr<FetchTask> task)
{
    const bool take = source_ == Source::take;

    auto sql = fmt::format(
        "SELECT row_to_json(t)::text FROM ("
        "  SELECT id, type, method, resource, headers,"
        "         convert_from(content, 'UTF-8') AS content,"
        "         done, fail, agent, profile, command, message, data"
        "  FROM {}({}::uuid)"
        ") t",
        take ? "http.take" : "http.request",
        pq_quote_literal(task->id));

    pool_.execute(sql,
        // on_result
        [this, task, take](std::vector<PgResult> results) {
            // Timed out while waiting for the row: failed already, not sent.
            if (task->timed_out)
                return;

            // The shutdown has begun: a row read now would go out with nobody
            // left to settle it. Not sent, it stays as the database has it.
            if (modules_.stopped()) {
                remove_task(task->id);
                return;
            }

            const bool none = results.empty() || !results[0].ok() ||
                              results[0].rows() == 0 || results[0].columns() == 0;

            // http.take: no row means do not send — gone, done, failed or
            // expired (take closes that one itself). Not a failure: no
            // http.fail, which would overwrite a done row, and no callback.
            if (none && take) {
                log_.debug("PGFetch: request {} is not to be sent — dropped", task->id);
                remove_task(task->id);
                return;
            }

            if (none) {
                do_fail(task, "http.request() returned empty result");
                return;
            }

            // Parse the JSON payload from PG
            const char* val = results[0].value(0, 0);
            if (!val || val[0] == '\0') {
                do_fail(task, "http.request() returned null");
                return;
            }

            try {
                task->payload = nlohmann::json::parse(val);
            } catch (const nlohmann::json::parse_error& e) {
                do_fail(task, fmt::format("JSON parse error: {}", e.what()));
                return;
            }

            do_curl(task);
        },
        // on_exception
        [this, task](std::string_view error) {
            if (task->timed_out)
                return;
            // Nothing was sent: while stopping, leave the row as it is.
            if (modules_.stopped()) {
                remove_task(task->id);
                return;
            }
            do_fail(task, fmt::format("PG error: {}", error));
        },
        false,
        // Only reads the row (http.take closes an expired one, which is the
        // same the second time): sent again after a lost connection, as before
        // T627 — otherwise a request would fail on a blip before it was sent.
        PgRetry::if_lost);
}

// ─── do_curl ────────────────────────────────────────────────────────────────

void PGFetch::do_curl(std::shared_ptr<FetchTask> task)
{
    const auto& p = task->payload;

    // Extract fields from payload JSON
    std::string url    = p.value("resource", "");
    std::string method = p.value("method", "GET");

    if (url.empty()) {
        do_fail(task, "empty resource URL");
        return;
    }

    // Request headers
    std::vector<std::pair<std::string, std::string>> headers;
    if (p.contains("headers") && p["headers"].is_object()) {
        for (auto& [k, v] : p["headers"].items()) {
            if (v.is_string())
                headers.emplace_back(k, v.get<std::string>());
        }
    }

    // Request body (may be base64-encoded in payload)
    std::string content;
    if (p.contains("content") && p["content"].is_string()) {
        content = p["content"].get<std::string>();
    }

    task->sent = true;
    fetch_.request(method, url, content, headers,
        // on_done
        [this, task](FetchResponse resp) {
            // The deadline or the stop came first: failure is reported,
            // callback made.
            if (task->timed_out) {
                log_.warn("PGFetch: request {} answered {} after {} — response dropped",
                          task->id, resp.status_code,
                          modules_.stopped() ? "the stop" : "its deadline");
                return;
            }
            do_done(task, resp);
        },
        // on_error
        [this, task](std::string_view error) {
            if (task->timed_out)
                return;
            do_fail(task, fmt::format("fetch error: {}", error));
        });
}

// ─── callbacks ──────────────────────────────────────────────────────────────

std::optional<std::string> PGFetch::callback_ident(std::string_view name)
{
    if (name.empty() || name.find('\0') != std::string_view::npos)
        return std::nullopt;

    const auto dot = name.find('.');
    if (dot == std::string_view::npos)
        return std::nullopt;

    const auto schema = name.substr(0, dot);
    const auto func   = name.substr(dot + 1);
    if (schema.empty() || func.empty() || func.find('.') != std::string_view::npos)
        return std::nullopt;

    return quote_ident(schema) + "." + quote_ident(func);
}

std::string PGFetch::callback_of(const FetchTask& task, const char* key) const
{
    const auto& p = task.payload;
    if (p.is_object() && p.contains(key) && p[key].is_string())
        return p[key].get<std::string>();
    return {};
}

// ─── do_done ────────────────────────────────────────────────────────────────
//
// The response and the done callback go in one query — one transaction: a
// callback that throws takes the response down with it, and the request is
// then stored again on its own and marked failed with the reason. Stored
// with state 2 while the callback's work was lost, it would look delivered.

void PGFetch::do_done(std::shared_ptr<FetchTask> task, const FetchResponse& resp)
{
    task->settling = true;

    // Build response headers JSON
    auto resp_headers_json = headers_to_json(resp.headers);

    // Store response via http.create_response(id, status, status_text, headers, body)
    // body parameter is bytea — the bytes as they came, hex-encoded
    auto body_sql = resp.body.empty()
        ? std::string("null")
        : bytea_sql(resp.body);

    auto store_sql = fmt::format(
        "SELECT http.create_response({}, {}, {}, {}::jsonb, {})",
        pq_quote_literal(task->id),
        resp.status_code,
        pq_quote_literal(std::string(status_text(
            static_cast<HttpStatus>(resp.status_code)))),
        pq_quote_literal(resp_headers_json),
        body_sql);

    auto finish = [this, task](std::vector<PgResult>) { remove_task(task->id); };
    auto lost   = [this, task](std::string_view error) {
        log_.error("PGFetch: request {}: response not stored: {}", task->id, error);
        remove_task(task->id);
    };

    // quiet: every store below carries the response as the far end sent it —
    // all headers and the whole body (hex, trivially decoded). Such bodies hold
    // credentials: an OAuth access_token, Stripe's PaymentIntent client_secret,
    // an OCPI partner's token from /credentials. PgPool logs statement text, and
    // a dedicated postgres.log keeps it at debug (T713).
    //
    // Every store below is sent again after a lost connection (T627): a
    // repeat of one that committed stops on http.response's primary key and
    // rolls back whole. Not repeated, a store lost before its commit would
    // leave the request in state 1 — and one with a lifetime (expire) for
    // http.sweep to send out a second time.
    const auto done_func = callback_of(*task, "done");
    if (done_func.empty()) {
        pool_.execute(store_sql, finish, lost, /*quiet=*/true, PgRetry::if_lost);
        return;
    }

    const auto ident = callback_ident(done_func);
    if (!ident) {
        auto msg = fmt::format("done callback '{}' is not a function name — not called", done_func);
        log_.error("PGFetch: request {}: {}", task->id, msg);
        pool_.execute(store_sql + "; " + fail_sql(task->id, msg), finish, lost, /*quiet=*/true, PgRetry::if_lost);
        return;
    }

    auto sql = fmt::format("{}; SELECT {}({})", store_sql, *ident, pq_quote_literal(task->id));

    // With the callback in the same transaction, the key also keeps it from
    // running twice, and the handler below only logs (its store hits the same
    // key). Not repeated, the handler would mark failed a request that may
    // have been delivered.
    pool_.execute(sql, finish,
        [this, task, store_sql, done_func, finish, lost](std::string_view error) {
            auto msg = fmt::format("response and done callback {} rolled back: {}",
                                   done_func, error);
            log_.error("PGFetch: request {}: {}", task->id, msg);
            pool_.execute(store_sql + "; " + fail_sql(task->id, msg), finish, lost,
                          /*quiet=*/true, PgRetry::if_lost);
        },
        /*quiet=*/true, PgRetry::if_lost);
}

// ─── do_fail ────────────────────────────────────────────────────────────────
//
// Marking the request failed (state 3) and the fail callback go in one
// transaction, as in do_done: a callback that throws is named in the error.

void PGFetch::do_fail(std::shared_ptr<FetchTask> task, std::string_view message)
{
    task->settling = true;

    const std::string msg(message);
    const auto mark_sql = fail_sql(task->id, msg);

    auto finish = [this, task](std::vector<PgResult>) { remove_task(task->id); };
    auto lost   = [this, task](std::string_view error) {
        log_.error("PGFetch: request {}: failure not stored: {}", task->id, error);
        remove_task(task->id);
    };

    const auto fail_func = callback_of(*task, "fail");
    if (fail_func.empty()) {
        pool_.execute(mark_sql, finish, lost);
        return;
    }

    const auto ident = callback_ident(fail_func);
    if (!ident) {
        auto why = fmt::format("{}; fail callback '{}' is not a function name — not called", msg, fail_func);
        log_.error("PGFetch: request {}: {}", task->id, why);
        pool_.execute(fail_sql(task->id, why), finish, lost);
        return;
    }

    auto sql = fmt::format("{}; SELECT {}({}, {})", mark_sql, *ident,
                           pq_quote_literal(task->id), pq_quote_literal(msg));

    pool_.execute(sql, finish,
        [this, task, msg, fail_func, finish, lost](std::string_view error) {
            auto why = fmt::format("{}; fail callback {} rolled back: {}", msg, fail_func, error);
            log_.error("PGFetch: request {}: {}", task->id, why);
            pool_.execute(fail_sql(task->id, why), finish, lost);
        });
}

// ─── check_timeouts ─────────────────────────────────────────────────────────

void PGFetch::check_timeouts(std::chrono::steady_clock::time_point now)
{
    for (auto& task : queue_) {
        // A task already settling is being stored or failed: failing it
        // again would race that query on another connection.
        if (task->in_progress && !task->settling && !task->timed_out && now >= task->deadline) {
            task->timed_out = true;
            do_fail(task, "request timeout");
        }
    }
}

// ─── remove_task ────────────────────────────────────────────────────────────

void PGFetch::remove_task(const std::string& id)
{
    queue_.erase(
        std::remove_if(queue_.begin(), queue_.end(),
            [&id](const auto& t) { return t->id == id; }),
        queue_.end());
}

} // namespace apostol

#endif // WITH_POSTGRESQL
