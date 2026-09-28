[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README.ru-RU.md)

Postgres Fetch
-

**Module** for **Apostol CRM**[^crm].

Description
-
**PGFetch** provides the ability to send HTTP requests in the PL/pgSQL programming language.

Outgoing requests
-
The module sends HTTP requests as signaled from the database.

Example:

~~~sql
-- Execute a request to yourself
SELECT http.fetch('http://localhost:8080/api/v1/time');
~~~

Outgoing requests are recorded in the `http.request` table, and the result of the request execution is stored in the `http.response` table.

To conveniently view outgoing requests and the responses received for them, use the `http.fetch` view:

~~~sql
SELECT * FROM http.fetch ORDER BY datestart DESC;
~~~

The `http.fetch()` function is asynchronous, and it returns a unique identifier for the outgoing request as a response.

Callback functions
-

In the `http.fetch()` function, you can pass the name of a callback function for processing a successful response or in the case of a failure.

~~~sql
SELECT * FROM http.fetch('http://localhost:8080/api/v1/time', done => 'http.done', fail => 'http.fail');
~~~

The callback functions must be created in advance. `done` accepts the unique identifier of the outgoing request (`uuid`); `fail` accepts the identifier and the error text (`uuid, text`).

The name is `schema.function`, as `http.fetch` checks it; PGFetch quotes each part as an identifier, so the name is never read as SQL. A name that is not of that shape is not called — the request is marked failed with the reason.

~~~sql
CREATE OR REPLACE FUNCTION http.done (
  pRequest  uuid
) RETURNS   void
AS $$
DECLARE
  r         record;
BEGIN
  SELECT method, resource, status, status_text, response INTO r FROM http.fetch WHERE id = pRequest;

  RAISE NOTICE '% % % %', r.method, r.resource, r.status, r.status_text;
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;
~~~

~~~sql
CREATE OR REPLACE FUNCTION http.fail (
  pRequest  uuid,
  pError    text
) RETURNS   void
AS $$
DECLARE
  r         record;
BEGIN
  SELECT method, resource, error INTO r FROM http.request WHERE id = pRequest;

  RAISE NOTICE 'ERROR: % % %', r.method, r.resource, r.error;
END;
$$ LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = http, pg_temp;
~~~

Database module
-

PGFetch is tightly coupled to the **`http`** database module — [db-http](https://github.com/apostoldevel/db-http).

Outgoing requests and their results are stored entirely in this module:

| Object | Purpose |
|--------|---------|
| `http.request` | Queued outgoing HTTP requests; an insert notifies channel `http`, PGFetch reads the row and dispatches it |
| `http.response` | Stores the HTTP response (status, headers, body) for each completed request |
| `http.fetch` (view) | Join of `http.request` + `http.response` for convenient inspection of request/response pairs |
| `http.fetch(resource, ...)` | PL/pgSQL function that enqueues a new outgoing request and returns its `uuid` |

> **Note:** PGFetch handles **outgoing** HTTP requests initiated from PL/pgSQL via `http.fetch()`. For **incoming** HTTP requests dispatched into PL/pgSQL, see [PGHTTP](https://github.com/apostoldevel/module-PGHTTP) — both modules share the same [db-http](https://github.com/apostoldevel/db-http) database module.

Function Parameters
-

To perform an HTTP request:
~~~sql
/**
 * Performs an HTTP request.
 * @param {text} resource - Resource
 * @param {text} method - Method
 * @param {jsonb} headers - HTTP headers
 * @param {bytea} content - Request content
 * @param {text} done - Name of callback function in case of successful response
 * @param {text} fail - Name of callback function in case of failure
 * @param {text} agent - Agent
 * @param {text} profile - Profile
 * @param {text} command - Command
 * @param {text} message - Message
 * @param {text} type - Sending method: native - native; curl - via cURL library
 * @param {text} data - Arbitrary data in JSON format
 * @return {uuid}**/
CREATE OR REPLACE FUNCTION http.fetch (
  resource      text,
  method        text DEFAULT 'GET',
  headers       jsonb DEFAULT null,
  content       bytea DEFAULT null,
  done          text DEFAULT null,
  fail          text DEFAULT null,
  agent         text DEFAULT null,
  profile       text DEFAULT null,
  command       text DEFAULT null,
  message       text DEFAULT null,
  type          text DEFAULT null,
  data          jsonb DEFAULT null
) RETURNS       uuid
~~~

Delivery and resend
-

A new row notifies channel `http`; PGFetch reads it with `http.take(id)` and sends it. No row back means *do not send* — the request is gone, already done or failed, or its lifetime has elapsed (`take` closes that one itself) — and the task is dropped without a failure or a callback.

A notification sent while PGFetch is not listening (the process restarting, PostgreSQL restarting) reaches no one. Every time LISTEN is (re)established PGFetch waits `sweep_age` seconds and calls `http.sweep(age, limit)`: requests older than any flight that are still unsent, **and carry a lifetime (`expire`)**, are handed out again, up to `max_attempts` times; those past their lifetime or attempts are closed on the way (state 3, no callback). A request without `expire` is never resent. Resend is at-least-once: a producer that must not repeat puts an idempotency key in the headers, or sets no `expire`.

The response and the `done` callback are stored in one transaction, as are the failure and the `fail` callback. A callback that throws takes its pair down with it; the request is then marked failed (state 3) with the callback's error, the response kept — never recorded as done while the callback's work was lost.

On a database without `http.take`/`http.sweep` (db-platform before 1.2.32), or with libapostol without `APOSTOL_PG_LISTEN_READY`, or with no `timeout`, PGFetch says so in the log and runs as before: reads by `http.request(id)`, no resend.

Configuration
-

```json
{
  "module": {
    "PGFetch": {
      "enable": true,
      "timeout": 30,
      "sweep_age": 90,
      "sweep_limit": 100
    }
  }
}
```

| Key | Default | Meaning |
|-----|---------|---------|
| `timeout` | none | seconds per request; without it a flight is unbounded and nothing is resent |
| `sweep_age` | 90 | seconds after LISTEN is up before `http.sweep`, and the age it passes; raised to `timeout + 30` if shorter |
| `sweep_limit` | 100 | rows per `http.sweep` call; a full batch is followed by another on the next heartbeat |

Installation
-

Follow the instructions for installing PostgreSQL in the description of [Apostol (C++20)](https://github.com/apostoldevel/libapostol#postgresql).

Follow the build and installation instructions for [Apostol (C++20)](https://github.com/apostoldevel/libapostol#build-and-installation).

[^crm]: **Apostol CRM** — a template project built on the [A-POST-OL](https://github.com/apostoldevel/libapostol) (C++20) and [PostgreSQL Framework for Backend Development](https://github.com/apostoldevel/db-platform) frameworks.
