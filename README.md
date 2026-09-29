# rekaf

*A small Kafka administration tool by Thoroughly Pizzled Studio.*

[Русский](README.ru.md)

rekaf is a Bash script that creates an Apache Kafka topic if needed and adds topic and consumer-group ACLs for a producer, a consumer, or both. It checks for existing resources before making changes.

Consumers receive topic and group `READ` permissions; producers receive topic `WRITE`. Client `CREATE` permission is intentionally not granted: the script creates the topic using administrative credentials.

## Documentation

| Guide | English | Русский |
| --- | --- | --- |
| User guide | [English](DOC/user-guide.en.md) | [Русский](DOC/user-guide.ru.md) |
| Developer guide | [English](DOC/developer-guide.en.md) | [Русский](DOC/developer-guide.ru.md) |

## License

[MIT License](LICENSE) — Copyright (c) 2026 Nikodem Zima.
