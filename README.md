[![ru](https://img.shields.io/badge/lang-ru-green.svg)](https://github.com/apostoldevel/module-PGFetch/blob/master/README.ru-RU.md)

Postgres Fetch
-

**PGFetch** - a module for [Apostol](https://github.com/apostoldevel/apostol).

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

The callback functions must be created in advance, and they must accept the unique identifier of the outgoing request (of type uuid) as a parameter.

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
  pRequest  uuid
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

Database installation
-
Follow the instructions for installing PostgreSQL in the description of [Apostol](https://github.com/apostoldevel/apostol#postgresql).

Module installation
-

Follow the instructions for building and installing [Apostol](https://github.com/apostoldevel/apostol#%D1%81%D0%B1%D0%BE%D1%80%D0%BA%D0%B0-%D0%B8-%D1%83%D1%81%D1%82%D0%B0%D0%BD%D0%BE%D0%B2%D0%BA%D0%B0).

Function Parameters
-

To performs HTTP request:
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