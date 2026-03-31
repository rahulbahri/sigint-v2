#!/usr/bin/env python3
"""
Run with: python worker.py
Or via Docker: rq worker --url $REDIS_URL

Starts an RQ worker that processes background jobs.
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

os.environ.setdefault("DATABASE_URL", os.environ.get("DATABASE_URL", ""))

from core.database import init_db
init_db()

import redis
from rq import Worker, Queue

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
conn = redis.from_url(REDIS_URL)
queues = [Queue(connection=conn)]

print(f"[Worker] Starting RQ worker, Redis: {REDIS_URL}")
w = Worker(queues, connection=conn)
w.work()
