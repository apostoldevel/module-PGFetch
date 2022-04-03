Postgres Query Fetch
-
**PQFetch** - модуль для [Апостол](https://github.com/apostoldevel/apostol).

Описание
-
Направляет HTTP GET и POST запросы в базу данных PostgreSQL вызывая функции http.get и http.post соответственно.

Парамерты функций
-
Для `GET` запроса:
~~~postgresql
/**
 * @param {text} patch - Путь
 * @param {jsonb} headers - HTTP заголовки
 * @param {jsonb} params - Параметры запроса
 * @return {SETOF json}
 */
CREATE OR REPLACE FUNCTION http.get (
  patch     text,
  headers   jsonb,
  params    jsonb DEFAULT null
) RETURNS   SETOF json
~~~ 

Для `POST` запроса:
~~~postgresql
/**
 * @param {text} patch - Путь
 * @param {jsonb} headers - HTTP заголовки
 * @param {jsonb} params - Параметры запроса
 * @param {jsonb} body - Тело запроса
 * @return {SETOF json}
 */
CREATE OR REPLACE FUNCTION http.post (
  patch     text,
  headers   jsonb,
  params    jsonb DEFAULT null,
  body      jsonb DEFAULT null
) RETURNS   SETOF json
~~~ 

Установка базы данных
-
Следуйте указаниям по установке PostgeSQL в описании [Апостол](https://github.com/apostoldevel/apostol#postgresql)

Установка модуля
-
Следуйте указаниям по сборке и установке [Апостол](https://github.com/apostoldevel/apostol#%D1%81%D0%B1%D0%BE%D1%80%D0%BA%D0%B0-%D0%B8-%D1%83%D1%81%D1%82%D0%B0%D0%BD%D0%BE%D0%B2%D0%BA%D0%B0)
