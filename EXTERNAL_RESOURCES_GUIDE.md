# External Resources Setup Guide

Everything the platform needs that isn't just code. For each item:
what it is, why you need it, how to get it, and what Claude Code will do with it.

---

## 1. STRIPE DEVELOPER ACCOUNT

### What Is It?
Stripe processes payments. Your customers use Stripe to collect money from their customers. You also use Stripe to bill your own customers for Axiom.

### Why You Need It
Two reasons:
1. **Connector:** Pull your customers' Stripe data into Axiom (their revenue, invoices, subscriptions)
2. **Billing:** Charge your customers for using Axiom

### What You Do (step by step):
1. Go to https://dashboard.stripe.com/register
2. Create account with your email
3. Verify your email
4. In the dashboard, make sure "Test mode" is ON (toggle at top-right)
5. Go to Developers > API Keys
6. Copy the **Secret key** (`sk_test_...`) and **Publishable key** (`pk_test_...`)
7. For webhooks (so Stripe tells Axiom when payments happen):
   - Go to Developers > Webhooks
   - Click "Add endpoint"
   - URL: `https://app.axiomsync.ai/api/stripe/webhook`
   - Select events: `checkout.session.completed`, `customer.subscription.deleted`
   - Click "Add endpoint"
   - Copy the **Webhook signing secret** (`whsec_...`)

### What You Give Claude Code:
Put these in your `.env` file:
```
STRIPE_SECRET_KEY=sk_test_...
STRIPE_PUBLISHABLE_KEY=pk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...
```

### What Claude Code Does With It:
- Already wired into `routers/billing.py` for your own billing
- Already wired into `connectors/stripe_connector.py` for customer data pulls
- Claude Code has added retry logic, rate limit handling, and proper error messages

### Cost: FREE (test mode is free, production charges 2.9% + $0.30 per transaction)

---

## 2. QUICKBOOKS DEVELOPER ACCOUNT

### What Is It?
QuickBooks is accounting software. Most small-to-medium US businesses use it. Your customers' financial data (invoices, expenses, revenue) lives here.

### Why You Need It
To pull accounting data from your customers' QuickBooks into Axiom, so the platform can compute financial KPIs.

### What You Do:
1. Go to https://developer.intuit.com/
2. Click "Sign Up" (or sign in if you have an Intuit account)
3. Click "Dashboard" > "Create an app"
4. Select "QuickBooks Online and Payments"
5. Name: "Axiom Intelligence"
6. Check "Accounting" scope
7. Click "Create app"
8. Go to "Keys & credentials" tab
9. Under "Development" (sandbox):
   - Copy **Client ID**
   - Click "Show" on Client Secret, copy it
10. Scroll to "Redirect URIs"
11. Add: `http://localhost:8003/api/quickbooks/callback`
12. Add: `https://app.axiomsync.ai/api/quickbooks/callback`
13. Click "Save"
14. Go to "Sandbox" in the left menu
15. Click "Add sandbox" (creates a fake QuickBooks company with data)

### What You Give Claude Code:
```
INTUIT_CLIENT_ID=ABc123...
INTUIT_CLIENT_SECRET=xyz789...
QB_REDIRECT_URI=http://localhost:8003/api/quickbooks/callback
```

### What Claude Code Does With It:
- OAuth2 flow is already implemented in `connectors/quickbooks_connector.py`
- Token refresh is now implemented (tokens expire every hour)
- The connector pulls: invoices, payments, customers, expenses, employees, accounts
- Data flows through transformer > canonical tables > KPI aggregator

### Cost: FREE (developer sandbox is free, production requires QuickBooks subscription ~$30/month for your test company, but your CUSTOMERS already have their own subscriptions)

---

## 3. XERO DEVELOPER ACCOUNT

### What Is It?
Xero is another accounting software, popular internationally and with startups.

### Why You Need It
Same as QuickBooks -- pull your customers' accounting data into Axiom.

### What You Do:
1. Go to https://developer.xero.com/
2. Create account
3. Go to "My Apps" > "New app"
4. App name: "Axiom Intelligence"
5. Integration type: "Web app"
6. URL: `https://app.axiomsync.ai`
7. Redirect URI: `http://localhost:8003/api/connectors/xero/callback`
8. Click "Create app"
9. Copy **Client ID**
10. Click "Generate a secret" -- copy it NOW (shown once only!)
11. For test data, go to https://www.xero.com/signup/ and create a free trial with demo company

### What You Give Claude Code:
```
XERO_CLIENT_ID=...
XERO_CLIENT_SECRET=...
XERO_REDIRECT_URI=http://localhost:8003/api/connectors/xero/callback
```

### What Claude Code Does With It:
- OAuth2 flow implemented
- 30-minute token refresh now implemented
- Pulls: invoices, contacts (customers), payments (revenue), accounts

### Cost: FREE (developer account free, Xero trial 30 days free)

---

## 4. GOOGLE CLOUD PROJECT (for Google OAuth SSO + Google Sheets)

### What Is It?
Google Cloud lets you use "Sign in with Google" for your app, and also lets you read customers' Google Sheets.

### Why You Need It
Two reasons:
1. **SSO:** Your customers want to click "Sign in with Google" instead of magic links
2. **Google Sheets connector:** Some customers track KPIs in spreadsheets

### What You Do:
1. Go to https://console.cloud.google.com/
2. Click the project dropdown (top bar) > "New Project"
3. Name: "Axiom Intelligence"
4. Click "Create"
5. **Enable APIs:**
   - Search bar: "Google Sheets API" > Click > Enable
   - Search bar: "Google Drive API" > Click > Enable
6. **Set up OAuth consent screen:**
   - Left sidebar: "APIs & Services" > "OAuth consent screen"
   - User type: "External"
   - App name: "Axiom Intelligence"
   - User support email: your email
   - Authorized domains: add `axiomsync.ai`
   - Developer contact: your email
   - Click through "Scopes" (add `email`, `profile`, `openid`, `spreadsheets.readonly`, `drive.metadata.readonly`)
   - Add test users: your email (and team emails)
   - Finish setup
7. **Create OAuth credentials:**
   - Left sidebar: "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth client ID"
   - Application type: "Web application"
   - Authorized redirect URIs:
     - `http://localhost:8003/api/auth/google/callback`
     - `https://app.axiomsync.ai/api/auth/google/callback`
     - `http://localhost:8003/api/connectors/google_sheets/callback`
     - `https://app.axiomsync.ai/api/connectors/google_sheets/callback`
   - Click "Create"
   - Copy **Client ID** and **Client Secret**

### What You Give Claude Code:
```
GOOGLE_CLIENT_ID=123456789.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=GOCSPX-...
GOOGLE_REDIRECT_URI=http://localhost:8003/api/connectors/google_sheets/callback
```

### What Claude Code Does With It:
- Wires up Google OAuth SSO as a login option (Claude Code will implement this)
- Google Sheets connector already exists, just needs these credentials
- Sheets connector reads spreadsheet data into canonical tables

### Cost: FREE (Google Cloud free tier covers OAuth and Sheets API for thousands of requests/day)

---

## 5. REDIS INSTANCE (for Production)

### What Is It?
Redis is a fast in-memory database. It's used for:
- Background job queue (syncing connectors, training forecast models)
- Rate limiting (preventing abuse)
- Caching (storing LLM-generated narratives so they load faster)

### Why You Need It
Right now, background jobs (connector sync, forecast training) don't run in production because there's no Redis. This means:
- Connectors can't sync automatically
- Forecast models can't be trained
- Everything happens synchronously (slow)

### What You Do:

**Option A: Upstash (Recommended - easiest, free tier)**
1. Go to https://upstash.com/
2. Create an account
3. Click "Create Database"
4. Name: "axiom-prod"
5. Region: US East (or wherever your Render server is)
6. Click "Create"
7. Copy the **Redis URL** (looks like `rediss://default:xxx@xxx.upstash.io:6379`)

**Option B: Render Redis add-on**
1. Go to your Render dashboard
2. Click "New" > "Redis"
3. Name: "axiom-redis"
4. Plan: Free (25MB, plenty for job queue)
5. Click "Create"
6. Copy the **Internal URL** from the Redis service page

### What You Give Claude Code:
```
REDIS_URL=rediss://default:xxx@xxx.upstash.io:6379
```
(Add this as an environment variable in Render dashboard too)

### What Claude Code Does With It:
- Background job queue (RQ) uses it for connector syncs and forecast training
- Rate limiting moves from in-memory (lost on restart) to Redis (persists)
- Narrative caching stores LLM results to avoid regenerating them

### Cost: FREE (Upstash free tier: 10K commands/day. Render free: 25MB)

---

## 6. RESEND EMAIL SERVICE

### What Is It?
Resend sends emails from your app. Used for magic link sign-in and KPI alert emails.

### Why You Need It
Without it, magic link authentication doesn't send emails. Users can't sign in.

### What You Do:
1. Go to https://resend.com/signup
2. Create an account
3. Go to "API Keys" in the sidebar
4. Click "Create API Key"
5. Name: "axiom-prod"
6. Permission: "Full access"
7. Copy the API key (starts with `re_...`)
8. **Set up your domain** (so emails come from @axiomsync.ai, not @resend.dev):
   - Go to "Domains" in the sidebar
   - Click "Add Domain"
   - Enter: `axiomsync.ai`
   - Resend will show you DNS records to add
   - Go to your domain registrar (wherever you bought axiomsync.ai)
   - Add the DNS records (usually 3 records: SPF, DKIM, DMARC)
   - Wait for verification (usually 5-30 minutes)
   - Once verified, emails will come from `noreply@axiomsync.ai`

### What You Give Claude Code:
```
RESEND_API_KEY=re_...
RESEND_FROM_EMAIL=noreply@axiomsync.ai
```

### What Claude Code Does With It:
- Magic link emails use Resend (already implemented)
- KPI alert emails use Resend (already implemented)
- Claude Code will improve email templates to look professional

### Cost: FREE (3,000 emails/month on free tier. That's plenty for 5 customers.)

---

## 7. ANTHROPIC API KEY (for AI Features)

### What Is It?
The Claude API powers all the AI features in Axiom: natural language KPI queries, narrative generation, smart actions, weekly briefings.

### Why You Need It
Without it, the AI features return empty results. The platform works for data display but loses its intelligence layer.

### What You Do:
1. Go to https://console.anthropic.com/
2. Sign in (or create account)
3. Go to "API Keys"
4. Click "Create Key"
5. Name: "axiom-prod"
6. Copy the key (starts with `sk-ant-...`)
7. Add billing: Go to "Plans & Billing" > add a payment method
8. Set a spending limit (e.g., $50/month to start -- each query costs ~$0.01-0.10)

### What You Give Claude Code:
```
ANTHROPIC_API_KEY=sk-ant-...
```

### What Claude Code Does With It:
- NLP query engine (ask questions about your KPIs in plain English)
- Weekly briefing generation
- Smart action recommendations
- KPI narrative generation

### Cost: Pay-per-use. ~$0.01-0.10 per query. $10-50/month for 5 active customers.

---

## 8. SENTRY ACCOUNT (Error Monitoring)

### What Is It?
Sentry catches errors in your production app and tells you about them before customers notice.

### Why You Need It
Without it, you won't know when things break in production. A customer's connector sync could be failing silently for days.

### What You Do:
1. Go to https://sentry.io/signup/
2. Create an account (free tier: 5K errors/month)
3. Create a new project:
   - Platform: Python
   - Project name: "axiom-backend"
4. Copy the **DSN** (looks like `https://xxx@oXXXXX.ingest.sentry.io/XXXXXXX`)

### What You Give Claude Code:
```
SENTRY_DSN=https://xxx@oXXXXX.ingest.sentry.io/XXXXXXX
```
(Add this in Render environment variables too)

### What Claude Code Does With It:
- Already integrated in `main.py` (Sentry SDK is imported and configured)
- Catches all unhandled exceptions
- Tracks performance (20% of transactions sampled)

### Cost: FREE (5,000 errors/month, 1 user, 30-day retention)

---

## 9. STAGING ENVIRONMENT ON RENDER

### What Is It?
A copy of your production app that you can test on without affecting real customers.

### Why You Need It
Right now, every code change goes straight to production. If something breaks, customers see it immediately.

### What You Do:
1. Go to https://dashboard.render.com/
2. Click "New" > "Web Service"
3. Connect the same GitHub repo
4. Name: "axiom-intelligence-v2-staging"
5. Branch: `staging` (you'll create this branch)
6. Use the same `render.yaml` settings
7. Set all the same environment variables BUT:
   - Use a DIFFERENT database (so staging data doesn't mix with production)
   - Use TEST keys for Stripe (not production keys)
8. **Create a staging database:**
   - In Render, click "New" > "PostgreSQL"
   - Name: "axiom-staging-db"
   - Plan: Free
   - Copy the Internal Database URL
   - Set it as `DATABASE_URL` for the staging service

### What You Give Claude Code:
Nothing -- Claude Code will create the `staging` branch and update CI/CD to deploy to staging.

### What Claude Code Does:
- Creates `staging` branch
- Updates CI to deploy staging branch to staging environment
- Adds staging URL to CORS origins

### Cost: FREE (Render free tier supports multiple services)

---

## 10. POSTGRESQL DATABASE (for Production)

### What Is It?
The real database where all customer data is stored in production.

### Why You Need It
Right now, the production app might be using SQLite (a simple file-based database that doesn't handle multiple users well). PostgreSQL is the real production database.

### What You Do:

**Option A: Render PostgreSQL (simplest)**
1. In Render dashboard, click "New" > "PostgreSQL"
2. Name: "axiom-prod-db"
3. Plan: Starter ($7/month) or free (with limitations)
4. Region: same as your web service
5. Click "Create"
6. Copy the **Internal Database URL**

**Option B: Neon (recommended - better free tier)**
1. Go to https://neon.tech/
2. Create an account
3. Create a new project
4. Name: "axiom-prod"
5. Region: US East
6. Copy the **Connection string** (starts with `postgres://...`)

### What You Give Claude Code:
```
DATABASE_URL=postgres://user:password@host/dbname
```
(Set this in Render environment variables)

### What Claude Code Does With It:
- Already works! The app detects `DATABASE_URL` and uses PostgreSQL automatically
- All SQL is translated via the dialect abstraction layer in `database.py`

### Cost:
- Render PostgreSQL: Free (256MB) or $7/month (1GB)
- Neon: Free (500MB, 100 hours/month compute)

---

## COMPLETE .env FILE TEMPLATE

After setting everything up, your `.env` file should look like this:

```bash
# Database (leave empty for local SQLite)
DATABASE_URL=

# Auth
JWT_SECRET=generate-a-random-64-character-string-here

# Email
RESEND_API_KEY=re_...
RESEND_FROM_EMAIL=noreply@axiomsync.ai
APP_URL=http://localhost:5173

# AI
ANTHROPIC_API_KEY=sk-ant-...

# Stripe (your billing)
STRIPE_SECRET_KEY=sk_test_...
STRIPE_PUBLISHABLE_KEY=pk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...

# QuickBooks
INTUIT_CLIENT_ID=...
INTUIT_CLIENT_SECRET=...
QB_REDIRECT_URI=http://localhost:8003/api/quickbooks/callback

# Xero
XERO_CLIENT_ID=...
XERO_CLIENT_SECRET=...
XERO_REDIRECT_URI=http://localhost:8003/api/connectors/xero/callback

# Google (SSO + Sheets)
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_REDIRECT_URI=http://localhost:8003/api/connectors/google_sheets/callback

# Salesforce
SALESFORCE_CLIENT_ID=...
SALESFORCE_CLIENT_SECRET=...
SALESFORCE_REDIRECT_URI=http://localhost:8003/api/connectors/salesforce/callback

# HubSpot
HUBSPOT_API_KEY=pat-...

# Shopify
SHOPIFY_ACCESS_TOKEN=shpat_...
SHOPIFY_SHOP_DOMAIN=your-store.myshopify.com

# Brex
BREX_API_TOKEN=...

# Ramp
RAMP_CLIENT_ID=...
RAMP_CLIENT_SECRET=...

# NetSuite
NETSUITE_ACCOUNT_ID=...
NETSUITE_CONSUMER_KEY=...
NETSUITE_CONSUMER_SECRET=...
NETSUITE_TOKEN_ID=...
NETSUITE_TOKEN_SECRET=...

# Sage Intacct
SAGE_SENDER_ID=...
SAGE_SENDER_PASSWORD=...

# Snowflake
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...

# Background Jobs
REDIS_URL=redis://localhost:6379

# Error Monitoring
SENTRY_DSN=...

# Credential Encryption
ENCRYPTION_KEY=generate-a-base64-fernet-key

# Access Control
ALLOWED_DOMAINS=axiomsync.ai
ADMIN_EMAIL=rahul@axiomsync.ai
```

---

## SETUP ORDER (Do these in this order!)

| Step | What | Time | Blocks |
|------|------|------|--------|
| 1 | PostgreSQL database (Neon or Render) | 5 min | Everything else |
| 2 | Redis (Upstash or Render) | 5 min | Background jobs |
| 3 | Resend email | 10 min | User sign-in |
| 4 | Anthropic API key | 5 min | AI features |
| 5 | Sentry | 5 min | Error visibility |
| 6 | Stripe | 15 min | Billing + connector |
| 7 | QuickBooks developer | 30 min | Accounting connector |
| 8 | Xero developer | 30 min | Accounting connector |
| 9 | Google Cloud project | 15 min | SSO + Sheets |
| 10 | Staging environment | 15 min | Safe testing |
| 11 | HubSpot | 20 min | CRM connector |
| 12 | Salesforce | 30 min | CRM connector |
| 13+ | Shopify, Brex, Ramp, etc. | As needed | Per customer |

**Total time: ~3 hours to set up everything for Tier 1 + 2**

---

## WHAT CLAUDE CODE DOES AFTER YOU SET THESE UP

Once you have the credentials in your `.env` file:

1. **Test each connector locally:**
   - Start backend: `cd backend && python3 -m uvicorn main:app --port 8003`
   - Start frontend: `cd frontend && npm run dev`
   - Go to Data Sources page in the app
   - Connect each service and trigger a sync

2. **Claude Code has already done:**
   - Added retry logic to all connectors
   - Fixed pagination bugs
   - Added token refresh for OAuth connectors
   - Added proper error handling
   - Created `.env.example` template
   - Fixed the auto-seed bug
   - Added RQ worker to production deployment
   - Hardened JWT validation

3. **After you test locally, deploy to production:**
   - Add all env vars to Render dashboard
   - Push to main branch
   - CI/CD automatically deploys
