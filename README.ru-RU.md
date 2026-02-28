[![en](https://img.shields.io/badge/lang-en-green.svg)](README.md)

Postgres Fetch
-
**PGFetch** — модуль для [Apostol](https://github.com/apostoldevel/apostol) + [db-platform](https://github.com/apostoldevel/db-platform) — **Apostol CRM**[^crm].

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

Для удобного просмотра исходящих запросов и полученных на них ответов воспользуйтесь представлением `http.fetch`:

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

Функции обратного вызова должны быть созданы заранее и в качестве параметра должны принимать уникальный номер исходящего запроса (тип `uuid`).

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

Модуль базы данных
-

PGFetch тесно связан с модулем **`http`** базы данных — [db-http](https://github.com/apostoldevel/db-http).

Исходящие запросы и их результаты хранятся исключительно в этом модуле:

| Объект | Назначение |
|--------|------------|
| `http.request` | Очередь исходящих HTTP-запросов; PGFetch опрашивает эту таблицу и отправляет каждую ожидающую запись |
| `http.response` | Хранит HTTP-ответ (статус, заголовки, тело) для каждого завершённого запроса |
| `http.fetch` (представление) | Объединение `http.request` + `http.response` для удобного просмотра пар запрос/ответ |
| `http.fetch(resource, ...)` | PL/pgSQL-функция, добавляющая новый исходящий запрос в очередь и возвращающая его `uuid` |

> **Примечание:** PGFetch обрабатывает **исходящие** HTTP-запросы, инициируемые из PL/pgSQL через `http.fetch()`. Для **входящих** HTTP-запросов, диспетчеризуемых в PL/pgSQL, используйте [PGHTTP](https://github.com/apostoldevel/module-PGHTTP) — оба модуля разделяют один и тот же модуль базы данных [db-http](https://github.com/apostoldevel/db-http).

Настройка
-

```json
{
  "modules": {
    "PGFetch": {
      "enabled": true
    }
  }
}
```

Установка базы данных
-
Следуйте указаниям по установке PostgreSQL в описании [Апостол](https://github.com/apostoldevel/apostol#postgresql).

Установка модуля
-
Следуйте указаниям по сборке и установке [Апостол](https://github.com/apostoldevel/apostol#build-and-installation).

Параметры функций
-

Для отправки HTTP-запроса:
~~~sql
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

[^crm]: **Apostol CRM** — абстрактный термин, а не самостоятельный продукт. Он обозначает любой проект, в котором совместно используются фреймворк [Apostol](https://github.com/apostoldevel/apostol) (C++) и [db-platform](https://github.com/apostoldevel/db-platform) через специально разработанные модули и процессы. Каждый фреймворк можно использовать независимо; вместе они образуют полноценную backend-платформу.
