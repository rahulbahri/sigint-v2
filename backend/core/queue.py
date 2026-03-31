"""
core/queue.py — RQ job queue with graceful fallback to synchronous execution.

If Redis is unavailable (dev without Redis, or REDIS_URL not set),
all jobs run synchronously in the calling thread so dev stays simple.
"""
import os
from typing import Callable

REDIS_URL = os.environ.get("REDIS_URL", "")

_queue = None
_fallback = not bool(REDIS_URL)


def get_queue():
    """Return the RQ Queue, or None if Redis is unavailable."""
    global _queue, _fallback
    if _fallback:
        return None
    if _queue is not None:
        return _queue
    try:
        import redis
        from rq import Queue
        conn = redis.from_url(REDIS_URL, socket_connect_timeout=2)
        conn.ping()
        _queue = Queue(connection=conn, default_timeout=600)
        print(f"[Queue] Connected to Redis at {REDIS_URL}")
        return _queue
    except Exception as e:
        print(f"[Queue] Redis unavailable ({e}), falling back to synchronous execution")
        _fallback = True
        return None


def enqueue(fn: Callable, *args, job_timeout: int = 600, **kwargs):
    """
    Enqueue fn(*args, **kwargs) on RQ, or run synchronously if Redis is unavailable.
    Returns an RQ Job object (or None for sync execution).
    """
    q = get_queue()
    if q is not None:
        try:
            return q.enqueue(fn, *args, job_timeout=job_timeout, **kwargs)
        except Exception as e:
            print(f"[Queue] Enqueue failed ({e}), running synchronously")
    # Fallback: run in a daemon thread (same as before, but only as last resort)
    import threading
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return None
