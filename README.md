[![ru](https://img.shields.io/badge/lang-ru-green.svg)](https://github.com/apostoldevel/module-PGFetch/blob/master/README.ru-RU.md)

Postgres Fetch
-

**PGFetch** - a module for [Apostol](https://github.com/apostoldevel/apostol).

Description
-
**PGFetch** provides the ability to receive and send HTTP requests using the PL/pgSQL programming language.

Incoming requests
-
The module directs incoming HTTP `GET` and `POST` requests to the PostgreSQL database by calling the `http.get` and `http.post` functions, respectively, to process them.

Incoming requests are recorded in the `http.log` table.

Outgoing requests
-
The module is capable of not only receiving HTTP requests but also sending them on a signal from the database.

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
Follow the instructions for installing PostgeSQL in the description of [Apostol](https://github.com/apostoldevel/apostol#postgresql).

Module installation
-

Follow the instructions for building and installing [Apostol](https://github.com/apostoldevel/apostol#%D1%81%D0%B1%D0%BE%D1%80%D0%BA%D0%B0-%D0%B8-%D1%83%D1%81%D1%82%D0%B0%D0%BD%D0%BE%D0%B2%D0%BA%D0%B0).

General information
-

* Base endpoint URL: [http://localhost:8080/api/v1](http://localhost:8080/api/v1);
  * The module only accepts requests whose path starts with `/api` (this can be changed in the source code).
* All endpoints return either a `JSON object` or a `JSON array` depending on the number of records in the response. This behavior can be changed by adding the `?data_array=true` parameter to the request, in which case the response will be a `JSON array` regardless of the number of records.

* Endpoint URL format:
~~~
http[s]://<hosthame>[:<port>]/api/<route>
~~~
 
## HTTP Status Codes
* HTTP `4XX` status codes are used for client-side errors - the problem is on the client side.
* HTTP `5XX` status codes are used for internal errors - the problem is on the server side. It is important **NOT** to consider this as a failure operation. The execution status is **UNKNOWN** and may be successful.
 
## Passing Parameters
* For `GET` endpoints, parameters should be sent as a `query string`.
* For `POST` endpoints, some parameters can be sent as a `query string`, and some as a request body:
* The following content types are allowed when sending parameters as a request `body`:
  * `application/x-www-form-urlencoded` for `query string`;
  * `multipart/form-data` for `HTML forms`;
  * `application/json` for `JSON`.
* Parameters can be sent in any order.

Function Parameters
-

To handle a `GET` request:
~~~sql
/**
* @param {text} path - Path
* @param {jsonb} headers - HTTP headers
* @param {jsonb} params - Query parameters
* @return {SETOF json}
**/
CREATE OR REPLACE FUNCTION http.get (
  path      text,
  headers   jsonb,
  params    jsonb DEFAULT null
) RETURNS   SETOF json
~~~

To handle a `POST` request:
~~~sql
/**
* @param {text} path - Path
* @param {jsonb} headers - HTTP headers
* @param {jsonb} params - Query parameters
* @param {jsonb} body - Request body
* @return {SETOF json}
**/
CREATE OR REPLACE FUNCTION http.post (
  path      text,
  headers   jsonb,
  params    jsonb DEFAULT null,
  body      jsonb DEFAULT null
) RETURNS   SETOF json
~~~

To send a `GET` or `POST` request:
~~~sql
/**
* Performs an HTTP request.
* @param {text} resource - Resource
* @param {text} method - Method
* @param {jsonb} headers - HTTP headers
* @param {text} content - Request content
* @param {text} done - Name of the callback function in case of a successful response
* @param {text} fail - Name of the callback function in case of a failure
* @return {uuid}
**/
CREATE OR REPLACE FUNCTION http.fetch (
  resource  text,
  method    text DEFAULT 'GET',
  headers   jsonb DEFAULT null,
  content   text DEFAULT null,
  done      text DEFAULT null,
  fail      text DEFAULT null
) RETURNS   uuid
~~~