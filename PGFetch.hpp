#pragma once

#ifdef WITH_POSTGRESQL

#include "apostol/fetch_client.hpp"
#include "apostol/http.hpp"
#include "apostol/logger.hpp"
#include "apostol/module.hpp"
#include "apostol/pg.hpp"

#include <chrono>
#include <deque>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>

namespace apostol
{

class Application;
class EventLoop;

// ─── PGFetch ─────────────────────────────────────────────────────────────────
//
// Helper module that listens for PG NOTIFY on channel "http", fetches
// outbound HTTP requests via FetchClient, and stores responses back in PG.
//
// Lifecycle:
//   on_start()  → probe for http.take/http.sweep, pool_.listen("http", ...)
//   on_notify() → parse request_id, enqueue FetchTask (once per id)
//   on_ready    → LISTEN confirmed: re-probe, sweep due in sweep_age seconds
//   heartbeat() → process_queue() + check_timeouts() + sweep when due
//   on_stop()   → pool_.unlisten("http")
//
// SQL functions used:
//   SELECT * FROM http.take('{id}'::uuid)       — the row to send; none = drop silently
//   SELECT * FROM http.sweep(age, limit)        — requests whose NOTIFY may be lost
//   SELECT * FROM http.request('{id}'::uuid)    — instead of take on a database without it
//   SELECT http.create_response(...)             — store response  } one transaction
//   SELECT "schema"."done"('{id}')               — done callback   }
//   SELECT http.fail('{id}'::uuid, '{message}')  — mark failed     } one transaction
//   SELECT "schema"."fail"('{id}', '{message}')  — fail callback   }
//
// Mirrors v1 CPGFetch from src/modules/Helpers/PGFetch/.
//
class PGFetch final : public Module
{
public:
    PGFetch(Application& app, EventLoop& loop);

    std::string_view name() const override { return "PGFetch"; }
    bool enabled() const override { return enabled_; }

    // PGFetch does not handle incoming HTTP requests — it's a helper.
    bool execute(const HttpRequest&, HttpResponse&) override { return false; }

    void on_start() override;
    void on_stop() override;
    void heartbeat(std::chrono::system_clock::time_point now) override;

    // "schema.func" → "schema"."func"; anything else — nullopt. The form
    // http.create_request accepts; parts are quoted as they are, matching the
    // check in http.fetch (nspname/proname compared exactly), so a callback
    // name is never SQL.
    static std::optional<std::string> callback_ident(std::string_view name);

private:
    // How a request row is read: unknown until the probe answers.
    enum class Source { unknown, take, request };

    struct FetchTask
    {
        std::string id;                // request UUID
        nlohmann::json payload;        // parsed from http.take()/http.request()
        bool in_progress{false};
        bool settling{false};          // do_done/do_fail under way: the deadline no longer applies
        bool timed_out{false};
        std::chrono::steady_clock::time_point deadline;
    };

    void on_notify(std::string_view channel, std::string_view payload);
    void on_listen_ready();
    void enqueue(std::string id);
    void probe();
    void do_sweep();
    void do_query(std::shared_ptr<FetchTask> task);
    void do_curl(std::shared_ptr<FetchTask> task);
    void do_done(std::shared_ptr<FetchTask> task, const FetchResponse& resp);
    void do_fail(std::shared_ptr<FetchTask> task, std::string_view message);

    std::string callback_of(const FetchTask& task, const char* key) const;

    void process_queue();
    void check_timeouts(std::chrono::steady_clock::time_point now);
    void remove_task(const std::string& id);

    PgPool&      pool_;
    Logger&      log_;
    FetchClient  fetch_;
    long        timeout_ms_;
    long        sweep_age_s_;
    long        sweep_limit_;
    bool        sweep_possible_;   // on_ready exists and timeout bounds a flight
    bool        enabled_;

    Source      source_{Source::unknown};
    bool        probing_{false};
    std::chrono::steady_clock::time_point next_probe_{};

    // A sweep covers what was lost before ready_at_ once it runs sweep_age
    // after it; sweep_started_ tells whether the one in flight did.
    std::optional<std::chrono::steady_clock::time_point> sweep_due_;
    std::optional<std::chrono::steady_clock::time_point> ready_at_;
    std::chrono::steady_clock::time_point sweep_started_{};
    bool        sweeping_{false};

    std::deque<std::shared_ptr<FetchTask>> queue_;

};

} // namespace apostol

#endif // WITH_POSTGRESQL
