[![en](https://img.shields.io/badge/lang-en-green.svg)](https://github.com/apostoldevel/module-PGFetch/blob/master/README.md)

Postgres Fetch
-
**PGFetch** - модуль для [Апостол](https://github.com/apostoldevel/apostol).

Описание
-
**PGFetch** предоставляет возможность отправлять HTTP-запросы на языке программирования PL/pgSQL.

Исходящие запросы
-

Модуль отправляет HTTP-запросы по сигналу из базы данных.

Пример:

~~~sql
-- Выполнить запрос к самому себе
SELECT http.fetch('http://localhost:8080/api/v1/time');
~~~

Исходящие запросы записываются в таблицу `http.request`, результат выполнения запроса будет сохранён в таблице `http.response`.

Для удобноного просмотра исходящих запросов и полученных на них ответов воспользуйтесь представлением `http.fetch`:

~~~sql
SELECT * FROM http.fetch ORDER BY datestart DESC; 
~~~

Функция `http.fetch()` асинхронная, в качестве ответа она вернёт уникальный номер исходящего запроса.

Функции обратного вызова
-

В функцию `http.fetch()` можно передать имя функции обратного вызова как для обработки успешного ответа так и в случае сбоя.

~~~sql
SELECT * FROM http.fetch('http://localhost:8080/api/v1/time', done => 'http.done', fail => 'http.fail');
~~~

Функции обратного вызова должна быть создана заранее и в качестве параметра она должна принимать уникальный номер исходящего запроса (тип `uuid`).

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

Установка базы данных
-
Следуйте указаниям по установке PostgreSQL в описании [Апостол](https://github.com/apostoldevel/apostol#postgresql)

Установка модуля
-
Следуйте указаниям по сборке и установке [Апостол](https://github.com/apostoldevel/apostol#%D1%81%D0%B1%D0%BE%D1%80%D0%BA%D0%B0-%D0%B8-%D1%83%D1%81%D1%82%D0%B0%D0%BD%D0%BE%D0%B2%D0%BA%D0%B0)

Парамерты функций
-

Для отправки HTTP-запроса:
~~~sql
--------------------------------------------------------------------------------
-- HTTP FETCH ------------------------------------------------------------------
--------------------------------------------------------------------------------
/**
 * Выполняет HTTP запрос.
 * @param {text} resource - Ресурс
 * @param {text} method - Метод
 * @param {jsonb} headers - HTTP заголовки
 * @param {bytea} content - Содержимое запроса
 * @param {text} done - Имя функции обратного вызова в случае успешного ответа
 * @param {text} fail - Имя функции обратного вызова в случае сбоя
 * @param {text} agent - Агент
 * @param {text} profile - Профиль
 * @param {text} command - Команда
 * @param {text} message - Сообщение
 * @param {text} type - Способ отправки: native - родной; curl - через библиотеку cURL
 * @param {text} data - Произвольные данные в формате JSON
 * @return {uuid}
 */
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