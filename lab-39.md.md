# Lab: WebSocket Frontend Integration & Deployment

## Introduction

In this lab, you will build and deploy a complete real-time task progress system.

A Flask API accepts a task request and sends the task to a Celery worker. The Celery worker processes the task and publishes lifecycle events to a dedicated Redis Pub/Sub channel. A Python Socket.IO server subscribes to that channel and forwards events to the correct browser client through a Socket.IO room.

Finally, Nginx is configured as a reverse proxy so that both normal HTTP traffic and Socket.IO WebSocket traffic are available through port `80`.

> **Poridhi VM Ready:** This version is designed to run and be tested directly on an Ubuntu-based Poridhi VM.

---

## Architecture

```text
                         ┌──────────────────────┐
                         │       Browser        │
                         │ HTML + Socket.IO JS  │
                         └──────────┬───────────┘
                                    │
                         HTTP / WebSocket
                                    │
                            ┌───────▼───────┐
                            │     Nginx     │
                            │      :80      │
                            └───────┬───────┘
                                    │
                     ┌──────────────┴──────────────┐
                     │                             │
                HTTP /tasks                  /socket.io/
                     │                             │
              ┌──────▼──────┐              ┌───────▼──────┐
              │    Flask    │              │  Socket.IO   │
              │    :5000    │              │    :5556     │
              └──────┬──────┘              └───────┬──────┘
                     │                              │
                     │ Celery task                  │ Subscribe
                     ▼                              │
              ┌──────────────┐                      │
              │    Redis     │◄─────────────────────┘
              │    :6379     │
              │ Broker +     │
              │ Pub/Sub      │
              └──────┬───────┘
                     │
                     │ Task execution
                     ▼
              ┌──────────────┐
              │    Celery    │
              │    Worker    │
              └──────────────┘
```

### Event flow

```text
Browser
   │
   │ POST /tasks
   ▼
Flask API
   │
   │ process_order.delay()
   ▼
Celery Worker
   │
   │ PUBLISH task:<task_id>
   ▼
Redis Pub/Sub
   │
   ▼
Socket.IO Server
   │
   │ task_update
   ▼
Browser
```

---

# 1. Learning Objectives

By the end of this lab, you will be able to:

1. Submit asynchronous tasks through a Flask API.
2. Execute tasks using Celery and Redis.
3. Publish custom task lifecycle events through Redis Pub/Sub.
4. Subscribe to per-task Redis channels.
5. Forward Redis events to Socket.IO clients.
6. Use Socket.IO rooms for task-specific routing.
7. Display real-time task progress in a browser.
8. Configure Nginx for HTTP and WebSocket traffic.
9. Run the complete stack using systemd on a Poridhi VM.
10. Verify the complete end-to-end flow.

---

# 2. Project Structure

```text
websocket-realtime-lab/
├── README.md
├── requirements.txt
├── celery_app.py
├── tasks.py
├── app.py
├── ws_server.py
│
├── static/
│   └── index.html
│
├── scripts/
│   ├── start_redis.sh
│   ├── start_worker.sh
│   ├── start_api.sh
│   ├── start_ws.sh
│   └── stop_all.sh
│
├── nginx/
│   └── websocket.conf
│
└── systemd/
    ├── celery-worker.service
    ├── flask-api.service
    └── ws-server.service
```

---

# 3. Prerequisites

Use an Ubuntu-based Poridhi VM.

Recommended:

```text
OS: Ubuntu 22.04+
RAM: 1 GB+
Python: 3.10+
Redis: 6+
Nginx: 1.18+
```

Check the environment:

```bash
python3 --version
git --version
nginx -v
redis-server --version
```

---

# 4. Install System Dependencies

Run:

```bash
sudo apt update
sudo apt install -y \
    python3 \
    python3-venv \
    python3-pip \
    git \
    curl \
    redis-server \
    nginx
```

Start Redis:

```bash
sudo systemctl enable --now redis-server
```

Verify:

```bash
redis-cli ping
```

Expected:

```text
PONG
```

---

# 5. Create the Project

Clone your repository:

```bash
cd ~
git clone <YOUR_REPOSITORY_URL> websocket-realtime-lab
cd websocket-realtime-lab
```

If the project already exists:

```bash
cd ~/websocket-realtime-lab
```

---

# 6. Create Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

Upgrade pip:

```bash
pip install --upgrade pip
```

---

# 7. `requirements.txt`

Create `requirements.txt`:

```text
Flask==3.0.3
flask-cors==4.0.1
celery==5.4.0
redis==5.0.8
python-socketio[asyncio]==5.11.3
uvicorn[standard]==0.30.6
gunicorn==22.0.0
```

Install:

```bash
pip install -r requirements.txt
```

Verify:

```bash
python -c "import flask, celery, redis, socketio, uvicorn, gunicorn; print('Dependencies OK')"
```

Expected:

```text
Dependencies OK
```

---

# 8. Create `celery_app.py`

```python
from celery import Celery

celery_app = Celery(
    "websocket_realtime_lab",
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/0",
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_send_task_events=True,
)
```

---

# 9. Create `tasks.py`

```python
import json
import random
import time
from datetime import datetime, timezone

import redis
from celery.utils.log import get_task_logger

from celery_app import celery_app

logger = get_task_logger(__name__)

redis_client = redis.Redis.from_url(
    "redis://127.0.0.1:6379/0",
    decode_responses=True,
)


def publish_state(task_id, state, **extra):
    payload = {
        "state": state,
        "task_id": task_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **extra,
    }

    redis_client.publish(
        f"task:{task_id}",
        json.dumps(payload),
    )


@celery_app.task(
    bind=True,
    name="tasks.process_order",
    autoretry_for=(RuntimeError,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 2},
)
def process_order(self, payload, fail_probability=0.0):
    task_id = self.request.id

    publish_state(
        task_id,
        "STARTED",
        payload=payload,
    )

    for step in range(1, 6):
        time.sleep(1)

        publish_state(
            task_id,
            "PROGRESS",
            payload=payload,
            progress=step,
            total=5,
            message=f"Step {step}/5 complete",
        )

    if random.random() < fail_probability:
        publish_state(
            task_id,
            "RETRY",
            error="Simulated failure; retrying task",
            retry=self.request.retries + 1,
            max_retries=self.max_retries,
        )
        raise RuntimeError("simulated failure")

    result = {
        "payload": payload,
        "status": "completed",
        "by": "celery",
    }

    publish_state(
        task_id,
        "SUCCESS",
        result=result,
    )

    return result
```

### Why explicit events?

Publishing events from the task gives complete control over the event payload.

Example:

```json
{
  "state": "PROGRESS",
  "task_id": "abc123",
  "progress": 3,
  "total": 5,
  "message": "Step 3/5 complete"
}
```

---

# 10. Create `app.py`

```python
import os

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from tasks import process_order


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

app = Flask(__name__, static_folder=None)

CORS(app)


@app.route("/", methods=["GET"])
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok")


@app.route("/tasks", methods=["POST"])
def submit_task():
    body = request.get_json(silent=True) or {}

    payload = body.get("payload", "demo")

    try:
        fail_probability = float(
            body.get("fail_probability", 0.0)
        )
    except (TypeError, ValueError):
        return jsonify(error="fail_probability must be a number"), 400

    if not 0 <= fail_probability <= 1:
        return jsonify(
            error="fail_probability must be between 0 and 1"
        ), 400

    result = process_order.delay(
        payload,
        fail_probability,
    )

    return jsonify(
        task_id=result.id,
        payload=payload,
        fail_probability=fail_probability,
    ), 202
```

---

# 11. Create `ws_server.py`

The WebSocket server maintains one Redis Pub/Sub listener per task.

```python
import asyncio
import json
import os

import redis.asyncio as redis_async
import socketio


REDIS_URL = os.environ.get(
    "REDIS_URL",
    "redis://127.0.0.1:6379/0",
)

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
)

app = socketio.ASGIApp(sio)

redis_client = redis_async.from_url(
    REDIS_URL,
    decode_responses=True,
)

subscribers = {}
refcounts = {}
locks = {}


def get_lock(task_id):
    if task_id not in locks:
        locks[task_id] = asyncio.Lock()

    return locks[task_id]


async def pump(task_id):
    room = f"task_{task_id}"
    channel = f"task:{task_id}"

    pubsub = redis_client.pubsub()

    try:
        await pubsub.subscribe(channel)

        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue

            try:
                payload = json.loads(message["data"])
            except Exception:
                payload = {
                    "state": "ERROR",
                    "raw": message["data"],
                }

            await sio.emit(
                "task_update",
                payload,
                room=room,
            )

    except asyncio.CancelledError:
        pass

    finally:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.close()
        except Exception:
            pass


@sio.event
async def connect(sid, environ, auth):
    await sio.emit(
        "connection_status",
        {"connected": True},
        to=sid,
    )


@sio.event
async def disconnect(sid):
    pass


@sio.on("subscribe_task")
async def subscribe_task(sid, data):
    task_id = (data or {}).get("task_id")

    if not task_id:
        await sio.emit(
            "task_update",
            {
                "state": "ERROR",
                "error": "task_id required",
            },
            to=sid,
        )
        return

    room = f"task_{task_id}"

    async with get_lock(task_id):
        await sio.enter_room(sid, room)

        refcounts[task_id] = (
            refcounts.get(task_id, 0) + 1
        )

        if (
            task_id not in subscribers
            or subscribers[task_id].done()
        ):
            subscribers[task_id] = asyncio.create_task(
                pump(task_id)
            )

    await sio.emit(
        "subscription_status",
        {
            "task_id": task_id,
            "subscribed": True,
        },
        to=sid,
    )


@sio.on("unsubscribe_task")
async def unsubscribe_task(sid, data):
    task_id = (data or {}).get("task_id")

    if not task_id:
        return

    room = f"task_{task_id}"

    async with get_lock(task_id):
        await sio.leave_room(sid, room)

        refcounts[task_id] = max(
            0,
            refcounts.get(task_id, 0) - 1,
        )

        if refcounts[task_id] == 0:
            task = subscribers.get(task_id)

            if task:
                task.cancel()

            subscribers.pop(task_id, None)
            refcounts.pop(task_id, None)
            locks.pop(task_id, None)
```

---

# 12. Important Pub/Sub Race Condition

Redis Pub/Sub does not store old messages.

Therefore:

```text
POST /tasks
      ↓
Celery starts immediately
      ↓
STARTED event
      ↓
Browser subscribes
```

can cause the browser to miss early events.

For this lab, we solve this at the frontend by subscribing immediately after receiving the task ID. The task also has a visible 5-second progress window, making the flow easy to observe.

For a production system where **no event may ever be lost**, use Redis Streams or another durable event mechanism instead of plain Pub/Sub.

---

# 13. Create `static/index.html`

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <title>Real-Time Task Progress</title>

    <script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>

    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 900px;
            margin: 40px auto;
            padding: 20px;
        }

        input, button {
            padding: 10px;
            margin: 5px;
        }

        button {
            cursor: pointer;
        }

        #status {
            margin: 20px 0;
            font-weight: bold;
        }

        .event {
            border: 1px solid #ddd;
            padding: 10px;
            margin: 8px 0;
            border-radius: 6px;
        }

        .progress-container {
            width: 100%;
            background: #eee;
            height: 20px;
            border-radius: 10px;
            overflow: hidden;
            margin-top: 10px;
        }

        #progress {
            height: 100%;
            width: 0%;
            background: #333;
            transition: width 0.3s;
        }

        pre {
            white-space: pre-wrap;
        }
    </style>
</head>

<body>

<h1>Real-Time Task Progress</h1>

<div id="status">
    Connecting...
</div>

<form id="task-form">

    <input
        id="payload"
        value="order-2001"
        placeholder="Payload"
        required
    >

    <input
        id="fail_probability"
        type="number"
        value="0"
        min="0"
        max="1"
        step="0.1"
    >

    <button type="submit">
        Submit Task
    </button>

</form>

<div class="progress-container">
    <div id="progress"></div>
</div>

<p id="task-id"></p>

<h2>Events</h2>

<div id="events"></div>

<script>
    const socket = io({
        transports: ["websocket", "polling"]
    });

    const statusEl = document.getElementById("status");
    const eventsEl = document.getElementById("events");
    const progressEl = document.getElementById("progress");
    const taskIdEl = document.getElementById("task-id");

    let currentTaskId = null;

    socket.on("connect", () => {
        statusEl.textContent = "Socket.IO Connected";
    });

    socket.on("disconnect", () => {
        statusEl.textContent = "Socket.IO Disconnected";
    });

    socket.on("connection_status", (data) => {
        console.log("Connection:", data);
    });

    socket.on("subscription_status", (data) => {
        console.log("Subscription:", data);
    });

    socket.on("task_update", (data) => {
        console.log("Task update:", data);

        const event = document.createElement("div");
        event.className = "event";

        event.innerHTML = `
            <strong>${data.state}</strong>
            <br>
            Task: ${data.task_id}
            <br>
            Time: ${data.timestamp || ""}
            <br>
            ${data.message || data.error || ""}
        `;

        eventsEl.prepend(event);

        if (data.state === "PROGRESS") {
            const percent =
                (data.progress / data.total) * 100;

            progressEl.style.width = percent + "%";
        }

        if (data.state === "SUCCESS") {
            progressEl.style.width = "100%";
        }
    });

    document
        .getElementById("task-form")
        .addEventListener("submit", async (event) => {

            event.preventDefault();

            eventsEl.innerHTML = "";
            progressEl.style.width = "0%";

            const payload =
                document.getElementById("payload").value;

            const failProbability =
                Number(
                    document.getElementById(
                        "fail_probability"
                    ).value
                );

            const response = await fetch("/tasks", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    payload: payload,
                    fail_probability: failProbability
                })
            });

            if (!response.ok) {
                const error = await response.json();
                alert(error.error || "Task submission failed");
                return;
            }

            const data = await response.json();

            currentTaskId = data.task_id;

            taskIdEl.textContent =
                "Task ID: " + currentTaskId;

            socket.emit(
                "subscribe_task",
                {
                    task_id: currentTaskId
                }
            );
        });
</script>

</body>
</html>
```

---

# 14. Create Startup Scripts

## `scripts/start_redis.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

sudo systemctl enable --now redis-server

echo "Redis is running."
redis-cli ping
```

## `scripts/start_worker.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source venv/bin/activate

exec celery \
    -A celery_app.celery_app \
    worker \
    --loglevel=info \
    --concurrency=2
```

## `scripts/start_api.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source venv/bin/activate

exec gunicorn \
    --bind 127.0.0.1:5000 \
    app:app
```

## `scripts/start_ws.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source venv/bin/activate

exec uvicorn \
    ws_server:app \
    --host 127.0.0.1 \
    --port 5556
```

## `scripts/stop_all.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

sudo systemctl stop celery-worker flask-api ws-server 2>/dev/null || true

echo "Application services stopped."
```

Make executable:

```bash
chmod +x scripts/*.sh
```

---

# 15. Manual Testing Before systemd

Before configuring systemd, test each component manually.

## Terminal 1 — Redis

```bash
./scripts/start_redis.sh
```

## Terminal 2 — Celery Worker

```bash
./scripts/start_worker.sh
```

Keep this terminal open.

## Terminal 3 — Flask API

```bash
./scripts/start_api.sh
```

Keep this terminal open.

## Terminal 4 — Socket.IO

```bash
./scripts/start_ws.sh
```

Keep this terminal open.

---

# 16. Test Flask Directly

Open another terminal:

```bash
curl -i http://127.0.0.1:5000/health
```

Expected:

```text
HTTP/1.1 200 OK
```

Then:

```bash
curl -X POST http://127.0.0.1:5000/tasks \
    -H "Content-Type: application/json" \
    -d '{"payload":"order-2001","fail_probability":0}'
```

Expected:

```json
{
    "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "payload": "order-2001",
    "fail_probability": 0.0
}
```

---

# 17. Test Redis Pub/Sub

Open another terminal:

```bash
redis-cli PSUBSCRIBE 'task:*'
```

Then submit:

```bash
curl -X POST http://127.0.0.1:5000/tasks \
    -H "Content-Type: application/json" \
    -d '{"payload":"redis-test","fail_probability":0}'
```

You should see events similar to:

```text
STARTED
PROGRESS 1/5
PROGRESS 2/5
PROGRESS 3/5
PROGRESS 4/5
PROGRESS 5/5
SUCCESS
```

---

# 18. Configure Nginx

Create:

```text
nginx/websocket.conf
```

with:

```nginx
upstream flask_api {
    server 127.0.0.1:5000;
}

upstream socketio_server {
    server 127.0.0.1:5556;
}

server {
    listen 80;
    server_name _;

    location /socket.io/ {
        proxy_pass http://socketio_server/socket.io/;

        proxy_http_version 1.1;

        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        proxy_read_timeout 600s;
        proxy_send_timeout 600s;
    }

    location / {
        proxy_pass http://flask_api;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

Enable:

```bash
sudo rm -f /etc/nginx/sites-enabled/default

sudo ln -sf \
    "$(pwd)/nginx/websocket.conf" \
    /etc/nginx/sites-enabled/websocket.conf
```

Test:

```bash
sudo nginx -t
```

Expected:

```text
syntax is ok
test is successful
```

Reload:

```bash
sudo systemctl reload nginx
```

---

# 19. Test Through Nginx

Health check:

```bash
curl -i http://127.0.0.1/health
```

Expected:

```text
HTTP/1.1 200 OK
```

Test homepage:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1/
```

Expected:

```text
200
```

Test Socket.IO polling:

```bash
curl -i \
"http://127.0.0.1/socket.io/?EIO=4&transport=polling"
```

Expected:

```text
HTTP/1.1 200 OK
```

---

# 20. Browser Test on Poridhi VM

Find the VM public IP.

```bash
curl -4 ifconfig.me
```

Suppose the result is:

```text
203.0.113.10
```

Open:

```text
http://203.0.113.10/
```

You should see:

```text
Real-Time Task Progress
Socket.IO Connected

Payload: order-2001
Fail Probability: 0

Submit Task
```

Click **Submit Task**.

Expected:

```text
STARTED

PROGRESS
Step 1/5 complete

PROGRESS
Step 2/5 complete

PROGRESS
Step 3/5 complete

PROGRESS
Step 4/5 complete

PROGRESS
Step 5/5 complete

SUCCESS
```

The progress bar should move:

```text
20%
40%
60%
80%
100%
```

---

# 21. Test Failure and Retry

Set:

```text
Fail Probability = 1
```

Submit the task.

Because the probability is `1`, the task will fail and Celery will retry.

Expected event flow:

```text
STARTED
PROGRESS 1/5
PROGRESS 2/5
PROGRESS 3/5
PROGRESS 4/5
PROGRESS 5/5
RETRY
STARTED
...
RETRY
STARTED
...
```

After the configured retry limit, the Celery task will finally fail.

Then test:

```text
Fail Probability = 0
```

Expected:

```text
STARTED
PROGRESS 1/5
PROGRESS 2/5
PROGRESS 3/5
PROGRESS 4/5
PROGRESS 5/5
SUCCESS
```

---

# 22. systemd Deployment

For a persistent deployment, use systemd.

First make sure the project is located at:

```text
/opt/websocket-realtime-lab
```

Create:

```bash
sudo mkdir -p /opt/websocket-realtime-lab
```

Copy the project:

```bash
sudo cp -r . /opt/websocket-realtime-lab/
```

Set ownership:

```bash
sudo chown -R www-data:www-data /opt/websocket-realtime-lab
```

---

# 23. Create `systemd/celery-worker.service`

```ini
[Unit]
Description=WebSocket Realtime Lab - Celery Worker
After=redis-server.service
Wants=redis-server.service

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/websocket-realtime-lab
Environment="PYTHONUNBUFFERED=1"

ExecStart=/opt/websocket-realtime-lab/venv/bin/celery \
    -A celery_app.celery_app \
    worker \
    --loglevel=info \
    --concurrency=2

Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

# 24. Create `systemd/flask-api.service`

```ini
[Unit]
Description=WebSocket Realtime Lab - Flask API
After=redis-server.service
Wants=redis-server.service

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/websocket-realtime-lab
Environment="PYTHONUNBUFFERED=1"

ExecStart=/opt/websocket-realtime-lab/venv/bin/gunicorn \
    --bind 127.0.0.1:5000 \
    app:app

Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

# 25. Create `systemd/ws-server.service`

```ini
[Unit]
Description=WebSocket Realtime Lab - Socket.IO Server
After=redis-server.service
Wants=redis-server.service

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/websocket-realtime-lab
Environment="PYTHONUNBUFFERED=1"

ExecStart=/opt/websocket-realtime-lab/venv/bin/uvicorn \
    ws_server:app \
    --host 127.0.0.1 \
    --port 5556

Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

# 26. Enable systemd Services

Copy:

```bash
sudo cp systemd/celery-worker.service /etc/systemd/system/
sudo cp systemd/flask-api.service /etc/systemd/system/
sudo cp systemd/ws-server.service /etc/systemd/system/
```

Reload:

```bash
sudo systemctl daemon-reload
```

Enable and start:

```bash
sudo systemctl enable --now \
    celery-worker \
    flask-api \
    ws-server
```

Check:

```bash
systemctl is-active celery-worker
systemctl is-active flask-api
systemctl is-active ws-server
```

Expected:

```text
active
active
active
```

---

# 27. Check Service Logs

Celery:

```bash
sudo journalctl -u celery-worker -f
```

Flask:

```bash
sudo journalctl -u flask-api -f
```

Socket.IO:

```bash
sudo journalctl -u ws-server -f
```

Nginx:

```bash
sudo journalctl -u nginx -f
```

Press:

```text
Ctrl+C
```

to stop following logs.

---

# 28. Final End-to-End Verification

Run:

```bash
systemctl is-active redis-server
systemctl is-active nginx
systemctl is-active celery-worker
systemctl is-active flask-api
systemctl is-active ws-server
```

All should return:

```text
active
```

Test:

```bash
curl -s http://127.0.0.1/health
```

Expected:

```json
{"status":"ok"}
```

Test homepage:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1/
```

Expected:

```text
200
```

Test Socket.IO:

```bash
curl -s -o /dev/null \
    -w "%{http_code}\n" \
    "http://127.0.0.1/socket.io/?EIO=4&transport=polling"
```

Expected:

```text
200
```

---

# 29. Browser Verification Matrix

| Test | Command / Action | Expected |
|---|---|---|
| Redis | `redis-cli ping` | `PONG` |
| Flask health | `curl http://127.0.0.1/health` | `{"status":"ok"}` |
| Homepage | Open VM IP | UI loads |
| Socket.IO | Browser DevTools | Connected |
| Task | Click Submit | Task ID appears |
| STARTED | Submit task | STARTED event |
| Progress | Wait | 1/5 → 5/5 |
| Success | Normal task | SUCCESS |
| Retry | `fail_probability=1` | RETRY events |
| Redis Pub/Sub | `PSUBSCRIBE task:*` | Events visible |
| Nginx | `nginx -t` | Successful |
| systemd | `systemctl is-active ...` | active |

---

# 30. Troubleshooting

## Problem: `ModuleNotFoundError: flask_cors`

Run:

```bash
source venv/bin/activate
pip install flask-cors==4.0.1
```

Then:

```bash
pip install -r requirements.txt
```

---

## Problem: Redis is not running

```bash
sudo systemctl restart redis-server
redis-cli ping
```

Expected:

```text
PONG
```

---

## Problem: Celery worker cannot import application

Check:

```bash
pwd
ls
```

You should see:

```text
celery_app.py
tasks.py
app.py
ws_server.py
```

Run:

```bash
source venv/bin/activate
celery -A celery_app.celery_app inspect ping
```

---

## Problem: Nginx returns `502 Bad Gateway`

Check:

```bash
sudo systemctl status flask-api
sudo systemctl status ws-server
```

Then:

```bash
curl http://127.0.0.1:5000/health
curl "http://127.0.0.1:5556/socket.io/?EIO=4&transport=polling"
```

If either backend is down, check:

```bash
sudo journalctl -u flask-api -n 50
sudo journalctl -u ws-server -n 50
```

---

## Problem: Homepage works but WebSocket does not

Check Nginx:

```bash
sudo nginx -t
```

Check Socket.IO:

```bash
sudo systemctl status ws-server
```

Check browser DevTools:

```text
Network
   ↓
socket.io
```

Make sure the request is going through:

```text
/socket.io/
```

---

## Problem: Redis monitoring shows nothing

Use:

```bash
redis-cli PSUBSCRIBE 'task:*'
```

Do not use:

```bash
redis-cli SUBSCRIBE 'task:*'
```

---

## Problem: Port already in use

Check:

```bash
sudo ss -lntp | grep -E ':80|:5000|:5556|:6379'
```

---

# 31. Teardown

Stop application services:

```bash
sudo systemctl stop celery-worker flask-api ws-server
```

Disable:

```bash
sudo systemctl disable celery-worker flask-api ws-server
```

For lab cleanup:

```bash
sudo rm -f /etc/systemd/system/celery-worker.service
sudo rm -f /etc/systemd/system/flask-api.service
sudo rm -f /etc/systemd/system/ws-server.service

sudo systemctl daemon-reload
```

Remove Nginx configuration:

```bash
sudo rm -f /etc/nginx/sites-enabled/websocket.conf
sudo nginx -t
sudo systemctl reload nginx
```

---

# 32. Important Production Note

Redis Pub/Sub is suitable for demonstrating real-time event forwarding, but it is not a durable message queue.

If the Socket.IO server is disconnected when a Pub/Sub event is published, that event is lost.

For systems where every event must be recoverable, consider:

```text
Redis Streams
Kafka
RabbitMQ
Database-backed event storage
```

For this lab, Redis Pub/Sub is intentionally used to demonstrate:

```text
Celery
   ↓
Redis Pub/Sub
   ↓
Socket.IO
   ↓
Browser
```

---

# 33. Final Result

After completing the lab, the user will have:

```text
Browser
   │
   │ HTTP
   ▼
Nginx
   │
   ├──────────────► Flask API
   │                     │
   │                     ▼
   │                  Celery
   │                     │
   │                     ▼
   │                   Redis
   │                     │
   │                     ▼
   └──────────────► Socket.IO
                         │
                         ▼
                      Browser
```

The browser receives task events in real time without polling.

Example:

```text
STARTED
   ↓
PROGRESS 1/5
   ↓
PROGRESS 2/5
   ↓
PROGRESS 3/5
   ↓
PROGRESS 4/5
   ↓
PROGRESS 5/5
   ↓
SUCCESS
```

This completes the WebSocket Frontend Integration & Deployment lab on a Poridhi VM.
