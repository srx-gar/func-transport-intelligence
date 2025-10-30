# func-transport-intelligence

Azure Function for daily SAP ZTDWR data sync to Transport Intelligence Platform.

## Overview

This Azure Function automatically processes daily SAP ZTDWR (Surat Perintah Jalan - Transport Order) extracts from GAR and ingests them into the Transport Intelligence PostgreSQL database.

**Architecture**: Event-driven serverless function triggered by blob uploads to Azure Storage.

### Features

- ✅ **Automatic Trigger**: Event Grid detects new `.dat` files in `hex-ztdwr/` container
- ✅ **Gzip Support**: Auto-detects and decompresses gzipped files
- ✅ **Data Validation**: Business rule validation with configurable error threshold
- ✅ **Idempotent Upsert**: Uses `ON CONFLICT` to handle retries safely
- ✅ **Partial Error Handling**: Processes valid rows, logs invalid ones
- ✅ **Materialized View Refresh**: Automatically refreshes MVs after ingestion
- ✅ **Sync Metadata Tracking**: Complete audit trail in `sync_metadata` table
- ✅ **Error Alerting**: Email notifications on failures (configurable)
- ✅ **Health Check Endpoint**: `/api/healthz` for monitoring
- ✅ **Manual HTTP Trigger**: `/api/sync-ztdwr` endpoint to reprocess files on demand

---

## Prerequisites

- **Azure Subscription**: `6119a975-951f-4410-bf2a-a83b6099f446`
- **Resource Group**: `poc-gar-blocktracker`
- **Storage Account**: `gartransportintelligence` (container: `data/hex-ztdwr/`)
- **PostgreSQL Database**: `poc-gar-db.postgres.database.azure.com`
  - Database: `transport_intelligence`
  - User: `ti_user`
- **Python**: 3.11+

---

## Local Development

### 1. Clone Repository

```bash
git clone git@github.com:srx-gar/func-transport-intelligence.git
cd func-transport-intelligence
```

### 2. Set Up Python Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Local Settings

```bash
cp local.settings.json.example local.settings.json
# Edit local.settings.json with your credentials
```

**local.settings.json**:
```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName=gartransportintelligence;AccountKey=<KEY>;EndpointSuffix=core.windows.net",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "DB_HOST": "poc-gar-db.postgres.database.azure.com",
    "DB_PORT": "5432",
    "DB_USER": "ti_user",
    "DB_PASSWORD": "<PASSWORD>",
    "DB_NAME": "transport_intelligence",
    "VALIDATION_ERROR_THRESHOLD": "0.10",
    "ENABLE_MV_REFRESH": "true",
    "ALERT_EMAIL": "your-email@company.com",
    "MANUAL_SYNC_CONTAINER": "data"
  }
}
```

Optional settings:
- `DB_SCHEMA` (default `public`)
- `DB_TABLE` (default `transport_documents`)

### 4. Run Locally

```bash
# Install Azure Functions Core Tools first
# https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local

func start
```

### 5. Test Locally

Upload a test file to your storage account:

```bash
az storage blob upload \
  --account-name gartransportintelligence \
  --container-name data \
  --name hex-ztdwr/test_ztdwr_$(date +%Y%m%d%H%M%S).dat \
  --file ./test_data/sample.dat
```

The function will trigger automatically.

---

## Manual Sync Trigger

Use the new HTTP endpoint when you need to re-run the ETL for an existing blob without re-uploading it.

1. **Get the function key** (once):
   ```bash
   az functionapp function keys list \
     --resource-group poc-gar-blocktracker \
     --name func-transport-intelligence \
     --function-name sync-ztdwr \
     --query "default" -o tsv
   ```

2. **Invoke the endpoint** (defaults to container `data` and folder `hex-ztdwr/`):
   ```bash
   curl -X POST \
     "https://func-transport-intelligence.azurewebsites.net/api/sync-ztdwr?code=<FUNCTION_KEY>" \
     -H "Content-Type: application/json" \
     -d '{
       "blob_name": "UPP-ZTDWR-20251016025401.dat"
     }'
   ```

   Optional payload fields:

   - `blob_path`: full path inside the container (e.g. `hex-ztdwr/UPP-ZTDWR-20251016025401.dat`)
   - `container`: override container name (defaults to `MANUAL_SYNC_CONTAINER` env, fallback `data`)

   Successful responses include the `sync_id`, status, and record counts:

   ```json
   {
     "sync_id": "sync_20251016_072530_ab12cd34",
     "status": "SUCCESS",
     "trigger": "http",
     "records_total": 542,
     "records_inserted": 520,
     "records_updated": 22
   }
   ```

   Failures return a JSON error payload and do not rethrow within the Function runtime, so they surface immediately to the caller.

> ℹ️ Set `MANUAL_SYNC_CONTAINER` in your configuration if the blobs live outside the default `data` container.

---

## Azure Deployment

### 1. Create Azure Function App

```bash
# Variables
RESOURCE_GROUP="poc-gar-blocktracker"
STORAGE_ACCOUNT="gartransportintelligence"
FUNCTION_APP_NAME="func-transport-intelligence"
LOCATION="southeastasia"

# Create Function App (Linux + Python 3.11)
az functionapp create \
  --resource-group $RESOURCE_GROUP \
  --consumption-plan-type EP1 \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --name $FUNCTION_APP_NAME \
  --storage-account $STORAGE_ACCOUNT \
  --os-type Linux
```

### 2. Configure Environment Variables

```bash
az functionapp config appsettings set \
  --name $FUNCTION_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --settings \
    "DB_HOST=poc-gar-db.postgres.database.azure.com" \
    "DB_PORT=5432" \
    "DB_USER=ti_user" \
    "DB_PASSWORD=<PASSWORD>" \
    "DB_NAME=transport_intelligence" \
    "VALIDATION_ERROR_THRESHOLD=0.10" \
    "ENABLE_MV_REFRESH=true" \
    "ALERT_EMAIL=ops-team@company.com" \
    "MANUAL_SYNC_CONTAINER=data"
```

### 3. Enable Managed Identity

```bash
# Enable system-assigned managed identity
az functionapp identity assign \
  --name $FUNCTION_APP_NAME \
  --resource-group $RESOURCE_GROUP

# Get the principal ID
PRINCIPAL_ID=$(az functionapp identity show \
  --name $FUNCTION_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query principalId -o tsv)

# Grant Storage Blob Data Reader role
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/6119a975-951f-4410-bf2a-a83b6099f446/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"
```

### 4. Set Up Event Grid Subscription

```bash
# Get Function App resource ID
FUNCTION_RESOURCE_ID=$(az functionapp show \
  --name $FUNCTION_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query id -o tsv)

# Create Event Grid subscription
az eventgrid event-subscription create \
  --name ztdwr-blob-upload \
  --source-resource-id "/subscriptions/6119a975-951f-4410-bf2a-a83b6099f446/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT" \
  --endpoint "$FUNCTION_RESOURCE_ID/functions/ztdwr_sync" \
  --endpoint-type azurefunction \
  --included-event-types Microsoft.Storage.BlobCreated \
  --subject-begins-with /blobServices/default/containers/data/blobs/hex-ztdwr/ \
  --subject-ends-with .dat
```

### 5. Deploy Function Code

**Option A: Using Azure Functions Core Tools**

```bash
func azure functionapp publish $FUNCTION_APP_NAME
```

**Option B: Using GitHub Actions (Recommended)**

See [CI/CD Setup](#cicd-setup) below.

---

## CI/CD Setup

### 1. Configure GitHub Secrets

Add the following secrets to your GitHub repository:

| Secret Name | Value |
|-------------|-------|
| `AZURE_CREDENTIALS` | Service principal JSON (see below) |
| `DB_HOST` | `poc-gar-db.postgres.database.azure.com` |
| `DB_PORT` | `5432` |
| `DB_USER` | `ti_user` |
| `DB_PASSWORD` | `<your-password>` |
| `DB_NAME` | `transport_intelligence` |
| `ALERT_EMAIL` | `ops-team@company.com` |

### 2. Create Azure Service Principal

```bash
az ad sp create-for-rbac --name "github-func-transport-intelligence" \
  --role contributor \
  --scopes /subscriptions/6119a975-951f-4410-bf2a-a83b6099f446/resourceGroups/poc-gar-blocktracker \
  --sdk-auth
```

Copy the JSON output and add it as `AZURE_CREDENTIALS` secret in GitHub.

### 3. Push GitHub Secrets Using `gh` CLI

```bash
# Set secrets
gh secret set AZURE_CREDENTIALS < azure-credentials.json
gh secret set DB_HOST --body "poc-gar-db.postgres.database.azure.com"
gh secret set DB_PORT --body "5432"
gh secret set DB_USER --body "ti_user"
gh secret set DB_PASSWORD --body "<your-password>"
gh secret set DB_NAME --body "transport_intelligence"
gh secret set ALERT_EMAIL --body "ops-team@company.com"
```

### 4. Trigger Deployment

Push to `main` branch to trigger automatic deployment:

```bash
git add .
git commit -m "Deploy Azure Function"
git push origin main
```

GitHub Actions will:
1. Build the function
2. Configure app settings
3. Deploy to Azure
4. Run health check

---

## Database Setup

### 1. Create `sync_metadata` Table

```sql
CREATE TABLE sync_metadata (
    id SERIAL PRIMARY KEY,
    sync_id VARCHAR(50) UNIQUE NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_path TEXT,
    file_size_bytes BIGINT,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    records_total INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    error_details JSONB,
    validation_errors JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    CHECK (status IN ('SUCCESS', 'FAILED', 'PARTIAL_SUCCESS', 'IN_PROGRESS'))
);

CREATE INDEX idx_sync_metadata_status ON sync_metadata(status);
CREATE INDEX idx_sync_metadata_started_at ON sync_metadata(started_at);
CREATE INDEX idx_sync_metadata_file_name ON sync_metadata(file_name);
```

### 2. Add Columns to `transport_documents`

```sql
ALTER TABLE transport_documents
ADD COLUMN IF NOT EXISTS sync_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS source_file VARCHAR(255);

CREATE INDEX idx_transport_docs_sync_id ON transport_documents(sync_id);
```

### 3. Primary Key

The table migration defines `surat_pengantar_barang` as the primary key. No additional unique constraint is required.

---

## Monitoring

### Health Check

```bash
curl https://func-transport-intelligence.azurewebsites.net/api/healthz
```

Expected response:
```json
{"status": "ok", "database": "healthy"}
```

### Check Sync Status (SQL)

```sql
-- Last 24 hours sync status
SELECT * FROM sync_metadata
WHERE started_at > NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;

-- Daily summary
SELECT
    DATE(started_at) as date,
    COUNT(*) as syncs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
    SUM(records_total) as total_records
FROM sync_metadata
WHERE started_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(started_at)
ORDER BY date DESC;
```

### Application Insights

This project supports Application Insights for structured telemetry (requests, traces, exceptions), live metrics, and diagnostics. The repository's `host.json` already configures sampling to limit telemetry volume:

```json
{
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "maxTelemetryItemsPerSecond": 20
      }
    }
  }
}
```

Below are step-by-step commands and useful queries for the `poc-gar-blocktracker` resource group and the `func-transport-intelligence` Function App.

1) Create an Application Insights component (if not already present)

```bash
# Variables (customise if needed)
RG="poc-gar-blocktracker"
AI_NAME="ai-func-transport-intelligence"
LOCATION="southeastasia"  # match your Function App region

# Create App Insights
az monitor app-insights component create \
  --app "$AI_NAME" \
  --location "$LOCATION" \
  --resource-group "$RG" \
  --application-type web
```

2) Get the connection string and attach it to the Function App

```bash
FUNCTION_APP="func-transport-intelligence"
RG="poc-gar-blocktracker"

# Get the Application Insights connection string
AI_CONN_STR=$(az monitor app-insights component show \
  --app "$AI_NAME" --resource-group "$RG" \
  --query connectionString -o tsv)

# Set the connection string on the Function App
az functionapp config appsettings set \
  --name "$FUNCTION_APP" \
  --resource-group "$RG" \
  --settings APPLICATIONINSIGHTS_CONNECTION_STRING="$AI_CONN_STR"
```

Note: newer Function runtimes prefer `APPLICATIONINSIGHTS_CONNECTION_STRING`. If you only have an instrumentation key, you can also set `APPINSIGHTS_INSTRUMENTATIONKEY` (legacy).

3) Verify telemetry and stream logs

- Live log stream (host logs):

```bash
# Stream host logs in real time
func azure functionapp logstream "$FUNCTION_APP"
# or via Azure CLI
az webapp log tail --name "$FUNCTION_APP" --resource-group "$RG"
```

- Application Insights live metrics: open the Azure Portal → Application Insights (the component you created) → Live Metrics Stream.

4) Useful Kusto queries (Application Insights Logs)

a) Recent function requests (last 24 hours):

```
requests
| where timestamp > ago(24h)
| where cloud_RoleName == "func-transport-intelligence" or name contains "/api/"
| project timestamp, name, resultCode, duration, operation_Id
| order by timestamp desc
```

b) Traces and log lines containing the pipeline function name:

```
traces
| where timestamp > ago(24h)
| where message contains "ztdwr_sync" or message contains "sync_"
| project timestamp, severityLevel, message, operation_Id
| order by timestamp desc
```

c) Exceptions and failed runs:

```
exceptions
| where timestamp > ago(24h)
| project timestamp, type, problemId, outerMessage, innermostMessage, operation_Id
| order by timestamp desc
```

d) Correlate traces with a request using operation_Id:

```
let op = "<operation_Id from requests table>";
requests | where operation_Id == op
| join kind=leftouter (
    traces | where operation_Id == op
) on operation_Id
```

5) Tips and diagnostics

- Correlation: The Functions runtime and Application Insights automatically set `operation_Id` for each invocation — use it to correlate requests, traces, and exceptions.
- Sampling: The `host.json` samplingSettings help control cost/volume. If you need full telemetry for a short time, temporarily disable sampling by updating the `host.json` or setting the `APPLICATIONINSIGHTS_SAMPLING_PERCENTAGE` app setting.
- Live debugging: Turn on Live Metrics when investigating latency or error spikes. For detailed snapshots of exceptions, enable Snapshot Debugger in Application Insights (note: requires proper SDK/instrumentation and connectivity).
- Alerts: Create Azure Monitor alerts on failed requests or on a custom log query over Application Insights (for example: alert when exceptions count > 0 in 5m window).

6) Example: Query recent sync runs from `sync_metadata` table (database)

Your app writes run-level metadata to the database table `sync_metadata`. Use this SQL to inspect recent runs:

```sql
SELECT sync_id, file_name, status, started_at, completed_at, records_total, records_inserted, records_failed
FROM sync_metadata
ORDER BY started_at DESC
LIMIT 50;
```

7) If you don't see telemetry

- Confirm `APPLICATIONINSIGHTS_CONNECTION_STRING` is present in Function App settings.
- Confirm the function app is running in a supported runtime and that `host.json` logging settings are not blocking traces.
- For platform logs, enable App Service logging via:

```bash
az webapp log config --name "$FUNCTION_APP" --resource-group "$RG" --application-logging true --level Information
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | (required) |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_USER` | Database user | (required) |
| `DB_PASSWORD` | Database password | (required) |
| `DB_NAME` | Database name | (required) |
| `DB_SCHEMA` | DB schema for target table | `public` |
| `DB_TABLE` | Target table name | `transport_documents` |
| `VALIDATION_ERROR_THRESHOLD` | Max error rate (0-1) | `0.10` (10%) |
| `ENABLE_MV_REFRESH` | Refresh MVs after sync | `true` |
| `ALERT_EMAIL` | Email for failure alerts | `ops-team@company.com` |
| `MANUAL_SYNC_CONTAINER` | Container for manual sync HTTP | `data` |

### Function Configuration

**host.json**:
- **Retry Policy**: Exponential backoff, 3 retries max
- **Timeout**: 10 minutes
- **Event Grid**: Max 3 delivery attempts with dead letter queue

---

## Troubleshooting

### Function Not Triggering

**Check Event Grid subscription**:
```bash
az eventgrid event-subscription show \
  --name ztdwr-blob-upload \
  --source-resource-id "<storage-account-resource-id>"
```

**Verify blob path matches filter**: `hex-ztdwr/*.dat`

### Decompression Fails

**Check file format**:
```bash
# Download file and check magic number
xxd -l 16 ft_mth_ussrv_trans_ztdwr_20251016150610.dat
# Gzip starts with: 1f 8b
```

### High Validation Error Rate

**Query validation errors**:
```sql
SELECT validation_errors
FROM sync_metadata
WHERE sync_id = '<sync_id>';
```

### Database Connection Issues

**Check PostgreSQL firewall**:
- Allow Azure services
- Or whitelist Function App outbound IPs

**Verify connection string**:
```bash
psql "host=poc-gar-db.postgres.database.azure.com port=5432 dbname=transport_intelligence user=ti_user password=<PASSWORD> sslmode=require"
```

---

## File Format

**Expected ZTDWR .dat Format**:

- **Encoding**: UTF-8 or Latin-1
- **Compression**: Gzip (optional, auto-detected)
- **Delimiter**: Tab (`\t`), Pipe (`|`), or multi-char `~|^`
- **Header Row**: Yes (first row)

**Sample**:
```
SPB_ID\tWAKTU_TIMBANG_TERIMA\tTGL_SPB\tDRIVER_NIK\tDRIVER_NAME\tNOPOL\t...
SPB001\t2025-10-16 08:30:00\t2025-10-16\tDRV001\tJohn Doe\tB1234XYZ\t...
```

**Validation Rules**:
- `BBM_ACTUAL`: 0 < value ≤ 500
- `KILOMETER_ACTUAL`: 0 < value ≤ 500
- `SPB_ID`: Non-null, unique (mapped to `surat_pengantar_barang`)
- `WAKTU_TIMBANG_TERIMA`: Valid datetime

---

## Cost

**Azure Function (Consumption Plan)**:
- ~$0.20/month for 1 daily sync (5 min execution)

**Event Grid**:
- Free (< 100K operations/month)

**Storage (Blob)**:
- ~$0.01/month for 30 files/month

**Total**: ~$0.21/month 💰

---

## Support

- **GitHub Issues**: https://github.com/srx-gar/func-transport-intelligence/issues
- **Documentation**: See `/02-data-sync-architecture.md`
- **Ops Team**: ops-team@company.com

---

## License

Internal use only - GAR Transport Intelligence Platform

---

## Related Repositories

- **Backend API**: [svc-transport-intelligence](https://github.com/srx-gar/svc-transport-intelligence)
- **Frontend**: [transport-intelligence](https://github.com/srx-gar/transport-intelligence)
- **Documentation**: [transport-intelligence-common](https://github.com/srx-gar/transport-intelligence-common)

## Local database setup

If you see errors about missing tables like `sync_metadata`, run the provided SQL to initialize a minimal schema for local development:

psql "$DB_CONN" -f init_db.sql

Where $DB_CONN is a Postgres connection string such as:

postgresql://user:password@localhost:5432/dbname

Alternatively, let the function attempt to create `sync_metadata` at runtime — the code will create the table on first insert if the running user has sufficient privileges.
