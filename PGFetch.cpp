#ifdef WITH_POSTGRESQL

#include "PGFetch.hpp"
#include "apostol/application.hpp"

#include "apostol/pg_utils.hpp"

#include <fmt/format.h>

namespace apostol
{

// ─── Construction ───────────────────────────────────────────────────────────

PGFetch::PGFetch(Application& app, EventLoop& loop)
    : pool_(app.db_pool())
    , fetch_(loop)
    , timeout_ms_(0)
    , enabled_(true)
{
    if (auto* cfg = app.module_config("PGFetch")) {
        if (cfg->contains("timeout") && (*cfg)["timeout"].is_number())
            timeout_ms_ = (*cfg)["timeout"].get<long>() * 1000;
    }
    if (timeout_ms_ > 0)
        fetch_.set_timeout(timeout_ms_);
}

// ─── Lifecycle ──────────────────────────────────────────────────────────────

void PGFetch::on_start()
{
    pool_.listen("http", [this](std::string_view ch, std::string_view payload) {
        on_notify(ch, payload);
    });
}

void PGFetch::on_stop()
{
    pool_.unlisten("http");
}

// ─── on_notify ──────────────────────────────────────────────────────────────

void PGFetch::on_notify(std::string_view /*channel*/, std::string_view payload)
{
    // Payload is the request_id (UUID string)
    if (payload.empty())
        return;

    auto task = std::make_shared<FetchTask>();
    task->id = std::string(payload);

    // Set deadline: timeout + 10s grace (mirrors v1 pattern)
    long effective_timeout = timeout_ms_ > 0 ? timeout_ms_ + 10000 : 60000;
    task->deadline = std::chrono::steady_clock::now()
                   + std::chrono::milliseconds(effective_timeout);

    queue_.push_back(std::move(task));
}

// ─── heartbeat ──────────────────────────────────────────────────────────────

void PGFetch::heartbeat(std::chrono::system_clock::time_point /*now*/)
{
    auto now_steady = std::chrono::steady_clock::now();

    process_queue();
    check_timeouts(now_steady);
}

// ─── process_queue ──────────────────────────────────────────────────────────

void PGFetch::process_queue()
{
    for (auto& task : queue_) {
        if (!task->in_progress) {
            task->in_progress = true;
            do_query(task);
        }
    }
}

// ─── do_query ───────────────────────────────────────────────────────────────

void PGFetch::do_query(std::shared_ptr<FetchTask> task)
{
    auto sql = fmt::format(
        "SELECT row_to_json(t)::text FROM ("
        "  SELECT id, type, method, resource, headers,"
        "         convert_from(content, 'UTF-8') AS content,"
        "         done, fail, agent, profile, command, message, data"
        "  FROM http.request({}::uuid)"
        ") t",
        pq_quote_literal(task->id));

    pool_.execute(sql,
        // on_result
        [this, task](std::vector<PgResult> results) {
            if (results.empty() || !results[0].ok() ||
                results[0].rows() == 0 || results[0].columns() == 0) {
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
            do_fail(task, fmt::format("PG error: {}", error));
        });
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

    fetch_.request(method, url, content, headers,
        // on_done
        [this, task](FetchResponse resp) {
            do_done(task, resp);
        },
        // on_error
        [this, task](std::string_view error) {
            do_fail(task, fmt::format("fetch error: {}", error));
        });
}

// ─── do_done ────────────────────────────────────────────────────────────────

void PGFetch::do_done(std::shared_ptr<FetchTask> task, const FetchResponse& resp)
{
    // Build response headers JSON
    auto resp_headers_json = headers_to_json(resp.headers);

    // Store response via http.create_response(id, status, status_text, headers, body)
    auto sql = fmt::format(
        "SELECT http.create_response({}, {}, {}, {}::jsonb, {})",
        pq_quote_literal(task->id),
        resp.status_code,
        pq_quote_literal(std::string(status_text(
            static_cast<HttpStatus>(resp.status_code)))),
        pq_quote_literal(resp_headers_json),
        pq_quote_literal(resp.body));

    pool_.execute(sql,
        [this, task](std::vector<PgResult> /*results*/) {
            remove_task(task->id);
        },
        [this, task](std::string_view /*error*/) {
            // Best effort — remove task even if storing response failed
            remove_task(task->id);
        });
}

// ─── do_fail ────────────────────────────────────────────────────────────────

void PGFetch::do_fail(std::shared_ptr<FetchTask> task, std::string_view message)
{
    // Check if there's a custom fail function in the payload
    std::string fail_func;
    if (task->payload.contains("fail") && task->payload["fail"].is_string())
        fail_func = task->payload["fail"].get<std::string>();

    std::string sql;
    if (!fail_func.empty()) {
        sql = fmt::format("SELECT {}({}, {})",
                          fail_func,
                          pq_quote_literal(task->id),
                          pq_quote_literal(std::string(message)));
    } else {
        sql = fmt::format("SELECT http.fail({}, {})",
                          pq_quote_literal(task->id),
                          pq_quote_literal(std::string(message)));
    }

    pool_.execute(sql,
        [this, task](std::vector<PgResult> /*results*/) {
            remove_task(task->id);
        },
        [this, task](std::string_view /*error*/) {
            remove_task(task->id);
        });
}

// ─── check_timeouts ─────────────────────────────────────────────────────────

void PGFetch::check_timeouts(std::chrono::steady_clock::time_point now)
{
    for (auto& task : queue_) {
        if (task->in_progress && now >= task->deadline) {
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
