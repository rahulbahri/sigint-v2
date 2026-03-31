"""
routers/billing.py — Stripe billing endpoints (/api/stripe/*).
"""
import json

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel as _BM2

from core.config import APP_URL, STRIPE_PUB, STRIPE_SECRET, STRIPE_WEBHOOK_SEC, _stripe
from core.database import _audit

router = APIRouter()


class CheckoutRequest(_BM2):
    price_id: str
    success_url: str = f"{APP_URL}?payment=success"
    cancel_url: str  = f"{APP_URL}?payment=cancelled"


@router.get("/api/stripe/config")
async def stripe_config():
    return {"publishable_key": STRIPE_PUB, "configured": bool(STRIPE_PUB)}


@router.post("/api/stripe/create-checkout-session")
async def create_checkout_session(body: CheckoutRequest):
    if not _stripe:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    try:
        session = _stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": body.price_id, "quantity": 1}],
            mode="subscription",
            success_url=body.success_url + "&session_id={CHECKOUT_SESSION_ID}",
            cancel_url=body.cancel_url,
        )
        return {"session_id": session.id, "url": session.url}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    if _stripe and STRIPE_WEBHOOK_SEC:
        try:
            event = _stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SEC)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid webhook signature")
    else:
        try:
            event = json.loads(payload)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid payload")
    event_type = event.get("type", "")
    if event_type == "checkout.session.completed":
        sess = event.get("data", {}).get("object", {})
        _audit("subscription_activated", "stripe", sess.get("id", ""),
               f"New subscription: {sess.get('customer_email', 'unknown')}")
    elif event_type == "customer.subscription.deleted":
        sub = event.get("data", {}).get("object", {})
        _audit("subscription_cancelled", "stripe", sub.get("id", ""), "Subscription cancelled")
    return {"received": True}


@router.get("/api/stripe/subscriptions")
async def list_subscriptions():
    if not _stripe:
        return {"configured": False, "subscriptions": []}
    try:
        subs = _stripe.Subscription.list(limit=20)
        return {
            "configured": True,
            "subscriptions": [
                {"id": s.id, "status": s.status, "customer": s.customer,
                 "current_period_end": s.current_period_end}
                for s in subs.data
            ]
        }
    except Exception as e:
        return {"configured": True, "error": str(e), "subscriptions": []}
