# 🚀 gRPC Key-Value Service on Java

Высокопроизводительный микросервис на **Java 20**, реализующий хранилище "ключ-значение". Система оптимизирована для работы с большими объемами данных и поддерживает интеграцию с **Tarantool**.

[![CI check](https://github.com/unt1tledd/taskVK/actions/workflows/main.yml/badge.svg)](https://github.com/unt1tledd/taskVK/actions/workflows/main.yml)

---

## 🛠 Технологический стек
* **Java 20** (Temurin SDK) — современный синтаксис и производительность.
* **gRPC / Protobuf** — эффективный бинарный протокол общения.
* **Tarantool 3.2** — база данных.
* **Docker & Docker Compose** — контейнеризация всей инфраструктуры.
* **Maven** — управление зависимостями и сборка.
* **JUnit 5 & AssertJ** — надежное тестирование.

---

## ✨ Ключевые возможности
- **Stress-Ready**: Обработка **5 000 000+** записей в одном спейсе.
- **Null Safety**: Поддержка `null` значений в поле `value`.
- **Batching**: Эффективная итерация по диапазонам (Range) без забивания памяти.
- **CI/CD**: Полный цикл автоматической сборки и тестирования через GitHub Actions.

---

## 🚀 Быстрый старт

### 1. Сборка проекта
```bash
mvn clean install
```

### 2. Запуск контейнера
```bash
docker-compose up --build
```
### 3. Тестирования
```bash
mvn test -Dtest=KVServerTest,StressTest
```
