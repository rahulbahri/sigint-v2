"""
routers/jobs.py — Job status endpoint for RQ background jobs.
Also exposes a manual digest trigger endpoint.
"""
from fastapi import APIRouter, HTTPException, Request

from core.deps import _get_workspace, _require_workspace

router = APIRouter()


@router.post("/api/digest/trigger", tags=["System"])
def trigger_digest(request: Request):
    """
    Manually trigger the nightly digest for the current workspace.
    Runs synchronously and returns the delivery result.
    """
    workspace_id = _require_workspace(request)
    try:
        from core.digest import run_digest_for_workspace
        result = run_digest_for_workspace(workspace_id)
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/jobs/{job_id}", tags=["System"])
def get_job_status(job_id: str, request: Request):
    """Check the status of a background job by RQ job ID."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        from core.queue import get_queue
        q = get_queue()
        if q is None:
            return {"status": "unknown", "message": "Job queue not available (sync mode)"}
        from rq.job import Job
        job = Job.fetch(job_id, connection=q.connection)
        return {
            "job_id": job_id,
            "status": job.get_status().value,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "enqueued_at": job.enqueued_at.isoformat() if job.enqueued_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "ended_at": job.ended_at.isoformat() if job.ended_at else None,
            "exc_info": job.exc_info if job.is_failed else None,
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Job not found: {e}")
