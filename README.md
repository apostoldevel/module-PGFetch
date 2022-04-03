Postgres Query Fetch
-
**PQFetch** - модуль для [Апостол](https://github.com/apostoldevel/apostol).

Описание
-

**PQFetch** Предоставляет возможность проектировать серверное приложение (backend) на языке программирования PL/pqSQL непосредственно в самой базе данных.

Модуль направляет HTTP `GET` и `POST` запросы в базу данных PostgreSQL вызывая функции `http.get` и `http.post` соответственно.

Основное преимущество это очень высокая скорость обработки большого количества запросов к API серверного приложения.

Установка базы данных
-
Следуйте указаниям по установке PostgeSQL в описании [Апостол](https://github.com/apostoldevel/apostol#postgresql)

Установка модуля
-
Следуйте указаниям по сборке и установке [Апостол](https://github.com/apostoldevel/apostol#%D1%81%D0%B1%D0%BE%D1%80%D0%BA%D0%B0-%D0%B8-%D1%83%D1%81%D1%82%D0%B0%D0%BD%D0%BE%D0%B2%D0%BA%D0%B0)

## Общая информация
* Базовая конечная точка (endpoint url): [http://localhost:8080/api/v1](http://localhost:8080/api/v1);
  * Модуль принимает только те запросы путь которых начинаются с `/api` (можно изменить в исходном коде).
* Все конечные точки возвращают: `JSON-объект` или `JSON-массив` в зависимости от количества записей в ответе. Изменить это поведение можно добавив в запрос параметр `?data_array=true` тогда ответ будет `JSON-массив` в независимости от количества записей;

### Формат endpoint url:
```
http[s]://<hosthame>[:<port>]/api/<route>
```

## HTTP коды возврата
* HTTP `4XX` коды возврата применимы для некорректных запросов - проблема на стороне клиента.
* HTTP `5XX` коды возврата используются для внутренних ошибок - проблема на стороне сервера. Важно **НЕ** рассматривать это как операцию сбоя. Статус выполнения **НЕИЗВЕСТЕН** и может быть успешным.

## Передача параметров
* Для `GET` конечных точек параметры должны быть отправлены в виде `строки запроса (query string)` .
* Для `POST` конечных точек, некоторые параметры могут быть отправлены в виде `строки запроса (query string)`, а некоторые в виде `тела запроса (request body)`:
* При отправке параметров в виде `тела запроса` допустимы следующие типы контента:
  * `application/x-www-form-urlencoded` для `query string`;
  * `multipart/form-data` для `HTML-форм`;
  * `application/json` для `JSON`.
* Параметры могут быть отправлены в любом порядке.

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
