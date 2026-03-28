# Key-value HTTP Server (persistent)

## Запуск

Для того чтобы запустить сервер:

```bash
python3 main.py
```

## Бенчмарк

Для того чтобы запустить бенчмарк установите `wrk`

- Замер write requests

```bash
wrk -t4 -c100 -d10s -s post.lua http://127.0.0.1:8080/records
```

- Замер read requests

```bash
wrk -t4 -c100 -d10s http://127.0.0.1:8080/records/f2d4e421-32c1-4df5-aa45-6fad5d37ea52
```

- Замер list (dump) requests

```bash
wrk -t4 -c100 -d10s http://127.0.0.1:8080/dump
```

Из зависимостей только `wrk`, БД нет
