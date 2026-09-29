# rekaf

*A small Kafka administration tool by Thoroughly Pizzled Studio.*

[English](README.md)

rekaf — Bash-скрипт, который при необходимости создаёт топик Apache Kafka и добавляет ACL для продюсера, консьюмера или обоих. Перед изменениями скрипт проверяет наличие ресурсов.

Консьюмер получает права `READ` для топика и consumer group; продюсер — `WRITE` для топика. Право `CREATE` клиентам намеренно не выдаётся: скрипт создаёт топик с административными учётными данными.

## Документация

| Руководство | Русский | English |
| --- | --- | --- |
| Документация пользователя | [Русский](DOC/user-guide.ru.md) | [English](DOC/user-guide.en.md) |
| Документация разработчика | [Русский](DOC/developer-guide.ru.md) | [English](DOC/developer-guide.en.md) |

## Лицензия

[Лицензия MIT](LICENSE) — Copyright (c) 2026 Nikodem Zima.
