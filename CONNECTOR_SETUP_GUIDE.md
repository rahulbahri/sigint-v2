# Connector Sandbox Setup Guide

## How This Guide Works

For each of the 12 connectors, you'll see:
- **What you do** (create accounts, click buttons, copy keys)
- **What Claude Code does** (write code, fix bugs, wire things up)
- **How to send test data** (put fake numbers in so you can see them flow through)

---

## TIER 1: MUST HAVE FOR FIRST 5 CUSTOMERS

These are the connectors your first customers will actually need.

---

### 1. STRIPE (Payments & Revenue)

**Time to set up: ~15 minutes**

#### What You Do (Step by Step):

1. **Go to** https://dashboard.stripe.com/register
2. **Create an account** using your email (or log in if you already have one)
3. **IMPORTANT:** Make sure you are in **Test Mode**
   - Look at the top-right of the Stripe dashboard
   - There's a toggle that says "Test mode" -- make sure it's ON (orange)
   - When test mode is on, everything is fake money. Nothing is real.
4. **Get your test API key:**
   - Click "Developers" in the left sidebar
   - Click "API keys"
   - You'll see two keys:
     - `Publishable key` -- starts with `pk_test_...`
     - `Secret key` -- click "Reveal test key" -- starts with `sk_test_...`
   - **Copy the Secret key** (the `sk_test_...` one)
5. **Put the key in your environment:**
   - Open your `.env` file in the backend folder
   - Add this line: `STRIPE_SECRET_KEY=sk_test_PASTE_YOUR_KEY_HERE`
   - Also add: `STRIPE_PUBLISHABLE_KEY=pk_test_PASTE_YOUR_KEY_HERE`

#### How to Send Test Data Into Stripe:

6. **Create fake customers:**
   - In Stripe dashboard, click "Customers" in the left sidebar
   - Click "+ Add customer"
   - Fill in a fake name like "Acme Corp" and email "test@acme.com"
   - Click "Add customer"
   - Do this 3-5 times with different fake companies

7. **Create fake payments:**
   - Click "Payments" in the left sidebar
   - Click "+ Create payment"
   - Amount: type any number like `5000` (this means $50.00 -- Stripe uses cents)
   - Customer: pick one of your fake customers
   - Card number: use `4242 4242 4242 4242` (this is Stripe's magic test card)
   - Expiry: any future date like `12/30`
   - CVC: any 3 digits like `123`
   - Click "Create payment"
   - Do this 10-20 times with different amounts and customers

8. **Create fake invoices:**
   - Click "Invoices" in the left sidebar
   - Click "+ Create invoice"
   - Pick a customer, add a line item (e.g., "Monthly subscription $500")
   - Click "Review invoice" then "Send invoice"
   - Do this 5-10 times

9. **Create fake subscriptions:**
   - Click "Products" in the left sidebar
   - Click "+ Add product"
   - Name: "Pro Plan", Price: $99/month (recurring)
   - Save it
   - Go to "Subscriptions" > "+ Create subscription"
   - Pick a customer and your Pro Plan product
   - Use test card `4242 4242 4242 4242`
   - Do this for 3-5 customers

10. **Test the connection:**
    - Start your backend: `cd backend && python3 -m uvicorn main:app --port 8003`
    - In another terminal: `curl -X POST http://localhost:8003/api/connectors/stripe/connect -H "Content-Type: application/json" -d '{"api_key": "sk_test_YOUR_KEY"}'`
    - Then trigger a sync: `curl -X POST http://localhost:8003/api/connectors/stripe/sync`
    - Check the response -- you should see your fake data

#### What Claude Code Already Did:
- Added retry logic with exponential backoff
- Added rate limit handling (Stripe returns 429 when you go too fast)
- Fixed error handling so one failed entity doesn't kill the whole sync
- Pagination already worked (cursor-based with `has_more`)

---

### 2. QUICKBOOKS (Accounting)

**Time to set up: ~30 minutes**

#### What You Do (Step by Step):

1. **Go to** https://developer.intuit.com/
2. **Create a developer account** (or sign in with your Intuit account)
3. **Create an app:**
   - Click "Dashboard" at the top
   - Click "Create an app"
   - Select "QuickBooks Online and Payments"
   - App name: "Axiom Intelligence" (or anything you want)
   - Scope: check "Accounting" (com.intuit.quickbooks.accounting)
   - Click "Create app"

4. **Get your credentials:**
   - You're now on your app's page
   - Click "Keys & credentials" tab
   - You'll see "Development" section (this is the sandbox):
     - **Client ID** -- copy this
     - **Client Secret** -- click "Show" and copy this

5. **Set up the redirect URL:**
   - Still on "Keys & credentials"
   - Scroll down to "Redirect URIs"
   - Click "Add URI"
   - Type: `http://localhost:8003/api/quickbooks/callback`
   - Click "Save"
   - ALSO add: `https://app.axiomsync.ai/api/quickbooks/callback` (for production)

6. **Put credentials in your environment:**
   - Open `.env` in the backend folder
   - Add:
     ```
     INTUIT_CLIENT_ID=PASTE_CLIENT_ID_HERE
     INTUIT_CLIENT_SECRET=PASTE_CLIENT_SECRET_HERE
     QB_REDIRECT_URI=http://localhost:8003/api/quickbooks/callback
     ```

7. **Create a QuickBooks sandbox company:**
   - Go back to https://developer.intuit.com/
   - Click "Dashboard"
   - Look for "Sandbox" in the left menu
   - Click "Add sandbox" (you might already have one)
   - This creates a fake company with fake data already in it!
   - Click on the sandbox company name to open it
   - You'll see a full QuickBooks with invoices, customers, expenses, etc.

#### How to Send Test Data Into QuickBooks:

8. **QuickBooks sandbox already has test data!** But you can add more:
   - In the sandbox QuickBooks, click "+ New" (green button, top left)
   - "Invoice" -- create a fake invoice to a customer
   - "Expense" -- add a fake expense
   - "Customer" -- add a fake customer
   - Create 10-15 transactions so there's enough data to compute KPIs

9. **Test the OAuth connection:**
   - Start your backend
   - Open a browser and go to: `http://localhost:8003/api/quickbooks/auth-url`
   - This will give you a URL -- open it in a new tab
   - You'll see the Intuit login page -- sign in with your developer account
   - Select your sandbox company
   - Click "Connect"
   - It will redirect back to your app with the data connected!

10. **Trigger a sync:**
    - `curl -X POST http://localhost:8003/api/quickbooks/sync`
    - This pulls all invoices, customers, expenses, payments from QuickBooks
    - Check your app's home page -- you should see KPIs populated

#### What Claude Code Already Did:
- Added automatic token refresh (QuickBooks tokens expire every hour)
- Added retry logic for failed API calls
- Fixed realm_id validation
- Added proper error logging

---

### 3. XERO (Accounting)

**Time to set up: ~30 minutes**

#### What You Do (Step by Step):

1. **Go to** https://developer.xero.com/
2. **Create a Xero developer account** (or sign in)
3. **Create a demo company:**
   - Click "My Apps" at the top
   - But FIRST, go to https://www.xero.com/signup/
   - Sign up for a free trial (you get 30 days free)
   - Choose "Demo Company" when asked
   - This gives you a Xero account with fake data

4. **Create a developer app:**
   - Go back to https://developer.xero.com/app/manage/
   - Click "New app"
   - App name: "Axiom Intelligence"
   - Integration type: "Web app"
   - Company or application URL: `https://app.axiomsync.ai`
   - Redirect URI: `http://localhost:8003/api/connectors/xero/callback`
   - Click "Create app"

5. **Get your credentials:**
   - On the app page, you'll see:
     - **Client ID** -- copy this
     - **Client Secret** -- click "Generate a secret" and copy it
   - IMPORTANT: Save the secret somewhere safe. Xero only shows it once!

6. **Put credentials in your environment:**
   - Open `.env` in the backend folder
   - Add:
     ```
     XERO_CLIENT_ID=PASTE_CLIENT_ID_HERE
     XERO_CLIENT_SECRET=PASTE_CLIENT_SECRET_HERE
     XERO_REDIRECT_URI=http://localhost:8003/api/connectors/xero/callback
     ```

7. **Add test data to your demo company:**
   - Log into your Xero trial at https://go.xero.com/
   - The demo company already has sample data
   - To add more: click "+New" and create invoices, bills, contacts
   - Create 10-15 items so there's good data to work with

8. **Test the OAuth connection:**
   - Start your backend
   - Go to: `http://localhost:8003/api/connectors/xero/auth-url`
   - Open the URL it gives you
   - Sign in with your Xero account
   - Select your demo company
   - Click "Allow access"
   - It redirects back -- you're connected!

9. **Trigger a sync:**
   - `curl -X POST http://localhost:8003/api/connectors/xero/sync`
   - Check your app for the new data

#### What Claude Code Already Did:
- Added automatic token refresh (Xero tokens expire every 30 minutes!)
- Added retry logic for API calls
- Fixed tenant_id handling for multi-org setups
- Added proper error handling per entity

---

## TIER 2: IMPORTANT FOR GROWTH-STAGE CUSTOMERS

---

### 4. HUBSPOT (CRM & Marketing)

**Time to set up: ~20 minutes**

#### What You Do:

1. **Go to** https://app.hubspot.com/signup-hubspot/crm
2. **Create a free HubSpot account** (the free CRM plan works fine)
3. **Create a Private App (easiest way):**
   - Go to Settings (gear icon, top right)
   - Left sidebar: "Integrations" > "Private Apps"
   - Click "Create a private app"
   - Name: "Axiom Intelligence"
   - Click "Scopes" tab
   - Check these boxes:
     - `crm.objects.contacts.read`
     - `crm.objects.deals.read`
     - `crm.objects.companies.read`
   - Click "Create app"
   - Copy the **Access Token** (starts with `pat-...`)

4. **Put the token in your environment:**
   - Open `.env`:
     ```
     HUBSPOT_API_KEY=pat-PASTE_YOUR_TOKEN_HERE
     ```

5. **Add test data:**
   - In HubSpot, click "Contacts" > "Create contact" (make 5-10 fake contacts)
   - Click "Sales" > "Deals" > "Create deal" (make 5-10 fake deals with dollar amounts)
   - Click "Contacts" > "Companies" > "Create company" (make 3-5 fake companies)

6. **Test:**
   - `curl -X POST http://localhost:8003/api/connectors/hubspot/connect -H "Content-Type: application/json" -d '{"api_key": "pat-YOUR_TOKEN"}'`
   - `curl -X POST http://localhost:8003/api/connectors/hubspot/sync`

---

### 5. SALESFORCE (CRM)

**Time to set up: ~30 minutes**

#### What You Do:

1. **Go to** https://developer.salesforce.com/signup
2. **Create a free Developer Edition account** (completely free, never expires)
3. **Wait for the email** -- Salesforce sends a verification email. Click the link.
4. **Create a Connected App:**
   - Log into your developer org
   - Click the gear icon (top right) > "Setup"
   - In the search box on the left, type "App Manager"
   - Click "App Manager"
   - Click "New Connected App"
   - Fill in:
     - Connected App Name: "Axiom Intelligence"
     - API Name: "Axiom_Intelligence"
     - Contact Email: your email
   - Check "Enable OAuth Settings"
   - Callback URL: `http://localhost:8003/api/connectors/salesforce/callback`
   - Add these OAuth scopes:
     - "Access and manage your data (api)"
     - "Perform requests on your behalf at any time (refresh_token, offline_access)"
   - Click "Save" (then "Continue")
   - **Wait 2-10 minutes** for Salesforce to activate it
   - Go back to the app and copy the **Consumer Key** and **Consumer Secret**

5. **Put credentials in your environment:**
   ```
   SALESFORCE_CLIENT_ID=PASTE_CONSUMER_KEY_HERE
   SALESFORCE_CLIENT_SECRET=PASTE_CONSUMER_SECRET_HERE
   SALESFORCE_REDIRECT_URI=http://localhost:8003/api/connectors/salesforce/callback
   ```

6. **Add test data:**
   - Salesforce Developer Edition comes with sample data
   - Go to "Accounts" tab -- you'll see sample companies
   - Go to "Opportunities" tab -- you'll see sample deals
   - Go to "Contacts" tab -- you'll see sample contacts
   - Add 5-10 more of each for richer data

7. **Test the OAuth connection:**
   - Go to `http://localhost:8003/api/connectors/salesforce/auth-url`
   - Sign in with your Salesforce developer account
   - Click "Allow"
   - Sync: `curl -X POST http://localhost:8003/api/connectors/salesforce/sync`

---

### 6. SHOPIFY (E-commerce)

**Time to set up: ~20 minutes**

#### What You Do:

1. **Go to** https://partners.shopify.com/signup
2. **Create a Shopify Partners account** (free)
3. **Create a development store:**
   - In the Partners dashboard, click "Stores" in the left sidebar
   - Click "Add store"
   - Choose "Development store"
   - Store name: anything (e.g., "axiom-test-store")
   - Choose a development store type
   - Click "Create"
4. **Create a Custom App:**
   - Go to your dev store's admin (click on the store)
   - Go to "Settings" > "Apps and sales channels" > "Develop apps"
   - Click "Create an app"
   - Name: "Axiom Intelligence"
   - Click "Configure Admin API scopes"
   - Check: `read_orders`, `read_customers`, `read_products`, `read_inventory`
   - Click "Save" then "Install app"
   - Copy the **Admin API access token** (shown once!)

5. **Put in your environment:**
   ```
   SHOPIFY_ACCESS_TOKEN=shpat_PASTE_TOKEN_HERE
   SHOPIFY_SHOP_DOMAIN=your-store-name.myshopify.com
   ```

6. **Add test data:**
   - In the Shopify admin, add fake products, create test orders, add customers
   - Use Shopify's "Bogus Gateway" for test payments

---

### 7. GOOGLE SHEETS (Manual Data Import)

**Time to set up: ~15 minutes**

#### What You Do:

1. **Go to** https://console.cloud.google.com/
2. **Create a new project** (or select existing)
   - Click the project dropdown at the top
   - Click "New Project"
   - Name: "Axiom Intelligence"
   - Click "Create"
3. **Enable the Google Sheets API:**
   - In the search bar at the top, type "Google Sheets API"
   - Click on it
   - Click "Enable"
4. **Also enable Google Drive API:**
   - Search "Google Drive API"
   - Click "Enable"
5. **Create OAuth credentials:**
   - Go to "APIs & Services" > "Credentials" (left sidebar)
   - Click "Create Credentials" > "OAuth client ID"
   - If asked to configure consent screen first:
     - Choose "External"
     - App name: "Axiom Intelligence"
     - User support email: your email
     - Developer contact: your email
     - Click "Save and Continue" through all steps
     - Add test users: your email
   - Back to Credentials > "Create Credentials" > "OAuth client ID"
   - Application type: "Web application"
   - Authorized redirect URIs: `http://localhost:8003/api/connectors/google_sheets/callback`
   - Click "Create"
   - Copy **Client ID** and **Client Secret**

6. **Put in your environment:**
   ```
   GOOGLE_CLIENT_ID=PASTE_CLIENT_ID_HERE
   GOOGLE_CLIENT_SECRET=PASTE_CLIENT_SECRET_HERE
   GOOGLE_REDIRECT_URI=http://localhost:8003/api/connectors/google_sheets/callback
   ```

7. **Create a test spreadsheet:**
   - Open Google Sheets (sheets.google.com)
   - Create a new sheet
   - Row 1 (headers): `month, revenue, cogs, customers, churn_rate, arr`
   - Fill in 12 rows of monthly data like:
     ```
     2025-01, 50000, 15000, 100, 3.5, 600000
     2025-02, 55000, 16000, 110, 3.2, 660000
     ... etc
     ```
   - Copy the spreadsheet ID from the URL (the long string between /d/ and /edit)

---

## TIER 3: NICE TO HAVE (Corporate Card / Expense)

---

### 8. BREX (Corporate Card)

**Time to set up: ~15 minutes**

#### What You Do:

1. **Go to** https://developer.brex.com/
2. **Sign up** for a developer account
3. **Create an API token:**
   - Go to Developer > API Keys
   - Click "Create Token"
   - Scopes: select read-only for accounts and transactions
   - Copy the token
4. **Put in environment:** `BREX_API_TOKEN=PASTE_TOKEN_HERE`
5. **Test data:** Brex sandbox comes with sample transactions
6. **Note:** Brex requires an actual Brex account for full access. The developer portal has limited sandbox.

---

### 9. RAMP (Expense Management)

**Time to set up: ~15 minutes**

#### What You Do:

1. **Go to** https://developer.ramp.com/
2. **Sign up** for developer access
3. **Create an OAuth app:**
   - Go to "Applications"
   - Create new app
   - Get **Client ID** and **Client Secret**
4. **Put in environment:**
   ```
   RAMP_CLIENT_ID=PASTE_HERE
   RAMP_CLIENT_SECRET=PASTE_HERE
   ```
5. **Test data:** Ramp provides a sandbox environment with sample transactions
6. **Note:** Ramp's developer program may require approval. Apply early.

---

## TIER 4: ENTERPRISE (ERP / Data Warehouse)

These are more complex. Most first-5 customers won't need them immediately.

---

### 10. NETSUITE (ERP)

**Time to set up: ~45 minutes** (most complex)

#### What You Do:

1. NetSuite requires an active account. There's no free sandbox.
2. If a customer uses NetSuite:
   - They need to create a **Token-Based Authentication** integration in their NetSuite admin
   - They give you: Account ID, Consumer Key, Consumer Secret, Token ID, Token Secret
3. **Put in environment:**
   ```
   NETSUITE_ACCOUNT_ID=PASTE_HERE
   NETSUITE_CONSUMER_KEY=PASTE_HERE
   NETSUITE_CONSUMER_SECRET=PASTE_HERE
   NETSUITE_TOKEN_ID=PASTE_HERE
   NETSUITE_TOKEN_SECRET=PASTE_HERE
   ```
4. **Test:** Use the customer's NetSuite sandbox if available (most NetSuite customers have one)

---

### 11. SAGE INTACCT (Accounting)

**Time to set up: ~30 minutes**

#### What You Do:

1. **Request a developer account** at https://developer.sage.com/
2. Sage Intacct requires a partner agreement for API access
3. If a customer uses Sage Intacct:
   - They provide: Company ID, User ID, User Password, Sender ID, Sender Password
4. **Put in environment:**
   ```
   SAGE_SENDER_ID=PASTE_HERE
   SAGE_SENDER_PASSWORD=PASTE_HERE
   SAGE_COMPANY_ID=PASTE_HERE
   SAGE_USER_ID=PASTE_HERE
   SAGE_USER_PASSWORD=PASTE_HERE
   ```
5. **Note:** Sage Intacct sandbox access is limited. Focus on Xero/QuickBooks first.

---

### 12. SNOWFLAKE (Data Warehouse)

**Time to set up: ~20 minutes**

#### What You Do:

1. **Go to** https://signup.snowflake.com/
2. **Create a free trial** (30 days, $400 in credits)
3. **Create a test database:**
   - Log into your Snowflake account
   - Click "Worksheets" > "+" (new worksheet)
   - Run these SQL commands:
     ```sql
     CREATE DATABASE axiom_test;
     USE DATABASE axiom_test;
     CREATE SCHEMA kpi_data;

     CREATE TABLE kpi_data.monthly_metrics (
       period VARCHAR,
       revenue FLOAT,
       cogs FLOAT,
       customers INT,
       churn_rate FLOAT,
       arr FLOAT
     );

     INSERT INTO kpi_data.monthly_metrics VALUES
       ('2025-01', 50000, 15000, 100, 3.5, 600000),
       ('2025-02', 55000, 16000, 110, 3.2, 660000),
       ('2025-03', 61000, 17000, 120, 2.9, 732000);
     ```
4. **Get your credentials:**
   - Account: the part before `.snowflakecomputing.com` in your URL
   - Username: what you signed up with
   - Password: what you set
5. **Put in environment:**
   ```
   SNOWFLAKE_ACCOUNT=xy12345.us-east-1
   SNOWFLAKE_USER=PASTE_HERE
   SNOWFLAKE_PASSWORD=PASTE_HERE
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=axiom_test
   SNOWFLAKE_SCHEMA=kpi_data
   ```

---

## PRIORITY ORDER: What to Set Up First

| Priority | Connector | Why | Time |
|----------|-----------|-----|------|
| 1st | **Stripe** | Every SaaS company uses it. Easiest to test. | 15 min |
| 2nd | **QuickBooks** | Most popular accounting for SMBs in the US | 30 min |
| 3rd | **Xero** | Popular accounting (especially international) | 30 min |
| 4th | **HubSpot** | Free CRM, easy to get pipeline data | 20 min |
| 5th | **Google Sheets** | Fallback for any data source | 15 min |
| 6th | **Salesforce** | Enterprise CRM | 30 min |
| 7th | **Shopify** | If any customer is e-commerce | 20 min |
| 8th | **Snowflake** | If customer has a data warehouse | 20 min |
| 9th-12th | Brex, Ramp, NetSuite, Sage | As needed per customer | varies |

---

## TESTING CHECKLIST

After setting up each connector, verify:

- [ ] Credentials stored and encrypted
- [ ] OAuth flow completes (for OAuth connectors)
- [ ] Sync pulls data without errors
- [ ] Data appears in canonical tables (check via `/api/canonical/{entity_type}`)
- [ ] KPI aggregator runs and produces monthly KPIs
- [ ] KPIs appear on the Home Screen
- [ ] Health Score updates with new data
