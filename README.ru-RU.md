[![en](https://img.shields.io/badge/lang-en-green.svg)](README.md)

Postgres Fetch
-
**Модуль** для **Apostol CRM**[^crm].

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

Функции обратного вызова должны быть созданы заранее. `done` принимает уникальный номер исходящего запроса (`uuid`), `fail` — номер и текст ошибки (`uuid, text`).

Имя — `схема.функция`, как его проверяет `http.fetch`; PGFetch квотирует каждую часть как идентификатор, так что имя никогда не читается как SQL. Имя другого вида не вызывается — запрос помечается неудачным с причиной.

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

Модуль базы данных
-

PGFetch тесно связан с модулем **`http`** базы данных — [db-http](https://github.com/apostoldevel/db-http).

Исходящие запросы и их результаты хранятся исключительно в этом модуле:

| Объект | Назначение |
|--------|------------|
| `http.request` | Очередь исходящих HTTP-запросов; вставка уведомляет канал `http`, PGFetch читает строку и отправляет её |
| `http.response` | Хранит HTTP-ответ (статус, заголовки, тело) для каждого завершённого запроса |
| `http.fetch` (представление) | Объединение `http.request` + `http.response` для удобного просмотра пар запрос/ответ |
| `http.fetch(resource, ...)` | PL/pgSQL-функция, добавляющая новый исходящий запрос в очередь и возвращающая его `uuid` |

> **Примечание:** PGFetch обрабатывает **исходящие** HTTP-запросы, инициируемые из PL/pgSQL через `http.fetch()`. Для **входящих** HTTP-запросов, диспетчеризуемых в PL/pgSQL, используйте [PGHTTP](https://github.com/apostoldevel/module-PGHTTP) — оба модуля разделяют один и тот же модуль базы данных [db-http](https://github.com/apostoldevel/db-http).

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

Доставка и повтор
-

Новая строка уведомляет канал `http`; PGFetch читает её через `http.take(id)` и отправляет. Пустой ответ означает *не слать* — запроса нет, он уже выполнен или отклонён, либо истёк его срок жизни (такой `take` закрывает сам), — и задача снимается без отказа и без колбэка.

Уведомление, отправленное, пока PGFetch не слушает (перезапуск процесса, перезапуск PostgreSQL), не доходит ни до кого. При каждом (пере)подключении LISTEN PGFetch ждёт `sweep_age` секунд и зовёт `http.sweep(age, limit)`: запросы старше любого полёта, ещё не отправленные **и со сроком жизни (`expire`)**, выдаются снова, не больше `max_attempts` раз; вышедшие за срок или попытки закрываются попутно (state 3, без колбэка). Запрос без `expire` не повторяется никогда. Повтор — «хотя бы один раз»: производитель, которому нельзя повторять, кладёт ключ идемпотентности в заголовки или не ставит `expire`.

Ответ и колбэк `done` сохраняются одной транзакцией, так же отказ и колбэк `fail`. Колбэк, выбросивший исключение, откатывает свою пару; запрос тогда помечается неудачным (state 3) с ошибкой колбэка, ответ сохраняется — выполненным при потерянной работе колбэка он не записывается.

На базе без `http.take`/`http.sweep` (db-platform до 1.2.32), с libapostol без `APOSTOL_PG_LISTEN_READY` или без `timeout` PGFetch пишет об этом в журнал и работает по-старому: чтение через `http.request(id)`, без повтора.

Настройка
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

| Ключ | По умолчанию | Смысл |
|------|--------------|-------|
| `timeout` | нет | секунд на запрос; без него полёт не ограничен и повтора нет |
| `sweep_age` | 90 | секунд после подъёма LISTEN до `http.sweep` и возраст, который он передаёт; если меньше `timeout + 30` — поднимается до него |
| `sweep_limit` | 100 | строк за вызов `http.sweep`; после полной партии — ещё один вызов на следующем heartbeat |

Установка
-

Следуйте указаниям по установке PostgreSQL в описании [Апостол (C++20)](https://github.com/apostoldevel/libapostol#postgresql).

Следуйте указаниям по сборке и установке [Апостол (C++20)](https://github.com/apostoldevel/libapostol#build-and-installation).

[^crm]: **Apostol CRM** — шаблон-проект построенный на фреймворках [A-POST-OL](https://github.com/apostoldevel/libapostol) (C++20) и [PostgreSQL Framework for Backend Development](https://github.com/apostoldevel/db-platform).
