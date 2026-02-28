#pragma once

#ifdef WITH_POSTGRESQL

#include "apostol/fetch_client.hpp"
#include "apostol/http.hpp"
#include "apostol/module.hpp"
#include "apostol/pg.hpp"

#include <chrono>
#include <deque>
#include <memory>
#include <nlohmann/json.hpp>
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
//   on_start()  → pool_.listen("http", ...)
//   on_notify() → parse request_id, enqueue FetchTask
//   heartbeat() → process_queue() + check_timeouts()
//   on_stop()   → pool_.unlisten("http")
//
// SQL functions used:
//   SELECT * FROM http.request('{id}'::uuid)    — get request payload
//   SELECT http.create_response(...)             — store response
//   SELECT http.fail('{id}'::uuid, '{message}')  — mark request failed
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

private:
    struct FetchTask
    {
        std::string id;                // request UUID
        nlohmann::json payload;        // parsed from http.request()
        bool in_progress{false};
        std::chrono::steady_clock::time_point deadline;
    };

    void on_notify(std::string_view channel, std::string_view payload);
    void do_query(std::shared_ptr<FetchTask> task);
    void do_curl(std::shared_ptr<FetchTask> task);
    void do_done(std::shared_ptr<FetchTask> task, const FetchResponse& resp);
    void do_fail(std::shared_ptr<FetchTask> task, std::string_view message);

    void process_queue();
    void check_timeouts(std::chrono::steady_clock::time_point now);
    void remove_task(const std::string& id);

    PgPool&      pool_;
    FetchClient  fetch_;
    long        timeout_ms_;
    bool        enabled_;

    std::deque<std::shared_ptr<FetchTask>> queue_;

    // Throttle heartbeat checks: check_date_ controls how often we
    // re-check the listener status (mirrors v1 m_CheckDate pattern)
    std::chrono::steady_clock::time_point check_date_{};
};

} // namespace apostol

#endif // WITH_POSTGRESQL
