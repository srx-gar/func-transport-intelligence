# Monitoring Guide - func-transport-intelligence

## Quick Health Check

### 1. Function App Health
```bash
curl https://func-transport-intelligence.azurewebsites.net/api/healthz
```

Expected response:
```json
{"status": "ok", "database": "healthy"}
```

### 2. Recent Blob Uploads
```bash
az storage blob list \
  --account-name gartransportintelligence \
  --container-name data \
  --prefix hex-ztdwr/ \
  --auth-mode key \
  --query "[].{Name:name, Size:properties.contentLength, LastModified:properties.lastModified}" \
  -o table
```

### 3. Blob Trigger Queue Status
```bash
az storage queue metadata show \
  --name azure-webjobs-blobtrigger-func-transport-intelligence \
  --account-name gartransportintelligence \
  --auth-mode key \
  --query approximateMessageCount -o tsv
```

- **0 messages**: Either all files processed OR no new files uploaded
- **> 0 messages**: Files waiting to be processed

---

## Database Monitoring

### Check Last 24 Hours Sync Status

```sql
SELECT
    sync_id,
    file_name,
    status,
    started_at,
    completed_at,
    records_total,
    records_inserted,
    records_updated,
    records_failed,
    duration_seconds
FROM sync_metadata
WHERE started_at > NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;
```

### Daily Summary (Last 30 Days)

```sql
SELECT
    DATE(started_at) as date,
    COUNT(*) as syncs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status = 'PARTIAL_SUCCESS' THEN 1 ELSE 0 END) as partial,
    SUM(records_total) as total_records,
    SUM(records_inserted) as inserted,
    SUM(records_updated) as updated,
    SUM(records_failed) as failed_records,
    AVG(duration_seconds) as avg_duration_sec
FROM sync_metadata
WHERE started_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(started_at)
ORDER BY date DESC;
```

### Recent Failures

```sql
SELECT
    sync_id,
    file_name,
    started_at,
    error_message,
    records_total,
    records_failed,
    validation_errors
FROM sync_metadata
WHERE status IN ('FAILED', 'PARTIAL_SUCCESS')
  AND started_at > NOW() - INTERVAL '7 days'
ORDER BY started_at DESC;
```

### Validation Error Patterns

```sql
SELECT
    jsonb_array_elements(validation_errors)->>'errors' as error_type,
    COUNT(*) as occurrence_count
FROM sync_metadata
WHERE validation_errors IS NOT NULL
  AND started_at > NOW() - INTERVAL '30 days'
GROUP BY error_type
ORDER BY occurrence_count DESC;
```

---

## Application Insights Queries

Access via Azure Portal → Application Insights → Logs

### Function Execution Logs

```kusto
traces
| where timestamp > ago(24h)
| where message contains "ztdwr_sync" or message contains "blob"
| order by timestamp desc
| take 100
```

### Function Execution Timeline

```kusto
requests
| where timestamp > ago(7d)
| where name == "ztdwr_sync"
| summarize count() by bin(timestamp, 1h), resultCode
| render timechart
```

### Error Analysis

```kusto
exceptions
| where timestamp > ago(7d)
| where cloud_RoleName == "func-transport-intelligence"
| summarize count() by problemId, outerMessage
| order by count_ desc
```

### Performance Metrics

```kusto
requests
| where timestamp > ago(7d)
| where name == "ztdwr_sync"
| summarize
    avg(duration),
    percentile(duration, 50),
    percentile(duration, 95),
    percentile(duration, 99)
| extend
    avg_minutes = avg_duration / 60000,
    p50_minutes = percentile_duration_50 / 60000,
    p95_minutes = percentile_duration_95 / 60000,
    p99_minutes = percentile_duration_99 / 60000
```

---

## Monitoring Script

Create `/tmp/monitor-func-transport.sh`:

```bash
#!/bin/bash
# Quick monitoring script for func-transport-intelligence

echo "=========================================="
echo "Azure Function Health Check"
echo "=========================================="
curl -s https://func-transport-intelligence.azurewebsites.net/api/healthz | python3 -m json.tool
echo ""

echo "=========================================="
echo "Recent Blob Uploads (hex-ztdwr/)"
echo "=========================================="
az storage blob list \
  --account-name gartransportintelligence \
  --container-name data \
  --prefix hex-ztdwr/ \
  --auth-mode key \
  --query "[].{Name:name, Size:properties.contentLength, LastModified:properties.lastModified}" \
  -o table 2>/dev/null | tail -n +3
echo ""

echo "=========================================="
echo "Blob Trigger Queue Status"
echo "=========================================="
QUEUE_LENGTH=$(az storage queue metadata show \
  --name azure-webjobs-blobtrigger-func-transport-intelligence \
  --account-name gartransportintelligence \
  --auth-mode key \
  --query approximateMessageCount -o tsv 2>/dev/null || echo "0")
echo "Messages waiting: $QUEUE_LENGTH"
echo ""

echo "=========================================="
echo "Last 5 Sync Operations"
echo "=========================================="
psql -h poc-gar-db.postgres.database.azure.com \
     -U ti_user \
     -d transport_intelligence \
     -c "SELECT sync_id, file_name, status, started_at, records_total, records_inserted, records_failed
         FROM sync_metadata
         ORDER BY started_at DESC
         LIMIT 5;"
```

Make executable and run:
```bash
chmod +x /tmp/monitor-func-transport.sh
/tmp/monitor-func-transport.sh
```

---

## Alerts Configuration

### Email Notifications

**Current Alert Email**: `rahadian.dewandono@srx.id`

**Alerts Sent For**:
1. Sync failure (status = FAILED)
2. High validation error rate (> threshold, default 10%)
3. Partial success with significant errors

**Email Format**:
- Subject: `[Alert] ZTDWR Sync Failed/Warning - {file_name}`
- Body includes:
  - Sync ID
  - File name
  - Error message
  - First 5 validation errors (if applicable)
  - Timestamp

### Azure Monitor Alerts (Recommended)

Create alerts in Azure Portal for:

**Alert 1: Function Execution Failures**
```
Resource: func-transport-intelligence
Metric: Failed Requests
Condition: > 0 in last 5 minutes
Action: Email rahadian.dewandono@srx.id
```

**Alert 2: No Sync in 24 Hours**
```
Type: Log query alert
Query:
  traces
  | where timestamp > ago(24h)
  | where message contains "Sync" and message contains "completed"
  | count
Condition: Count < 1
Action: Email alert
```

**Alert 3: High Error Rate**
```
Type: Log query alert
Query:
  traces
  | where timestamp > ago(1h)
  | where message contains "FAILED" or message contains "PARTIAL_SUCCESS"
  | count
Condition: Count > 3
Action: Email alert
```

---

## Troubleshooting

### Function Not Triggering

1. **Check blob trigger queue**:
   ```bash
   az storage queue metadata show \
     --name azure-webjobs-blobtrigger-func-transport-intelligence \
     --account-name gartransportintelligence \
     --auth-mode key
   ```

2. **Verify file naming**: Files must match `hex-ztdwr/*.dat`

3. **Check function app logs**:
   ```bash
   az webapp log tail --name func-transport-intelligence --resource-group poc-gar-blocktracker
   ```

### No Data in sync_metadata

**Possible causes**:
1. Function hasn't been triggered yet
2. Database connection issue
3. Table doesn't exist

**Check**:
```sql
-- Verify table exists
SELECT * FROM information_schema.tables WHERE table_name = 'sync_metadata';

-- Check database connectivity from function
-- Look at healthz endpoint response
```

### High Validation Error Rate

**Query specific sync errors**:
```sql
SELECT
    sync_id,
    file_name,
    validation_errors
FROM sync_metadata
WHERE status = 'PARTIAL_SUCCESS'
  AND started_at > NOW() - INTERVAL '7 days'
ORDER BY started_at DESC;
```

**Common validation errors**:
- `BBM_ACTUAL out of range`: Fuel > 500L or < 0
- `KILOMETER_ACTUAL out of range`: Distance > 500km or < 0
- `SPB_ID is null`: Missing transport order ID
- Invalid datetime format in `WAKTU_TIMBANG_TERIMA`

**Resolution**:
1. Contact GAR SAP team to fix data quality at source
2. Or adjust validation rules if they're too strict (edit `helpers/validator.py`)

### Function Running Slowly

**Check execution duration**:
```sql
SELECT
    file_name,
    records_total,
    duration_seconds,
    (duration_seconds / 60.0) as duration_minutes
FROM sync_metadata
WHERE started_at > NOW() - INTERVAL '7 days'
ORDER BY duration_seconds DESC;
```

**If > 5 minutes**:
1. Check PostgreSQL performance (indexes, connection pool)
2. Review materialized view refresh time
3. Consider increasing batch size in `helpers/postgres_client.py`

---

## Daily Operations Checklist

### Morning Check (5 minutes)

1. ✅ Verify healthz endpoint is responding
2. ✅ Check yesterday's sync completed successfully
3. ✅ Review any validation errors in sync_metadata
4. ✅ Verify blob trigger queue is empty (no backlog)

### Weekly Review (15 minutes)

1. ✅ Review 7-day sync success rate
2. ✅ Analyze validation error patterns
3. ✅ Check average execution duration trends
4. ✅ Review Application Insights for any warnings

### Monthly Review (30 minutes)

1. ✅ Analyze 30-day sync statistics
2. ✅ Review cost (should be ~$0.21/month)
3. ✅ Check storage account size growth
4. ✅ Update validation rules if needed
5. ✅ Review and optimize any slow queries

---

## Key Metrics to Track

| Metric | Target | Alert If |
|--------|--------|----------|
| Success Rate | > 95% | < 90% |
| Execution Duration | < 5 min | > 10 min |
| Validation Error Rate | < 5% | > 10% |
| Syncs per Day | 1 | 0 for 24h |
| Database Health | Healthy | Unhealthy |

---

## Contact

**Ops Team**: rahadian.dewandono@srx.id
**Documentation**: `/common/technical/architecture/02-data-sync-architecture.md`
**GitHub**: https://github.com/srx-gar/func-transport-intelligence
