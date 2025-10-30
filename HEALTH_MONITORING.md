# Health Monitoring Extension

The AgendaMetricsCollector now supports health monitoring through SQL-based checks.

## Configuration

### Constructor Options

```javascript
const metricsCollector = new AgendaMetricsCollector(agenda, {
  // ... existing options
  sequelize: sequelizeInstance,                    // Sequelize instance for running SQL queries
  healthMetricsDirectory: '/path/to/health/configs', // Directory containing JSON health check configs
  healthCheckInterval: 30000                       // Health check interval in ms (default: 30 seconds)
});
```

### Health Check Configuration Files

Create paired JSON and SQL files in the `healthMetricsDirectory`:

**JSON Configuration (e.g., `not_sent_tcg_note.json`):**
```json
{
  "alert_name": "not_sent_tcg_note",
  "title": "Unsent TCG Notes",
  "description": "Alert when TCG notes have not been sent for an extended period",
  "enabled": true
}
```

**SQL Query (e.g., `not_sent_tcg_note.sql`):**
```sql
SELECT COUNT(*) as total 
FROM your_table 
WHERE condition
```

**Requirements:**
- JSON and SQL files must have the same base name (e.g., `alert.json` and `alert.sql`)
- The SQL query must return a single row with a `total` field
- If `total > 0`, the health check is considered unhealthy (metric value = 1)
- If `total = 0`, the health check is considered healthy (metric value = 0)
- The `enabled` field defaults to `true` if not specified
- Disabled health checks (`enabled: false`) are skipped during execution

## Enabling/Disabling Health Checks

You can easily enable or disable individual health checks by setting the `enabled` flag:

```json
{
  "alert_name": "maintenance_mode_check",
  "description": "Temporarily disabled during maintenance",
  "enabled": false
}
```

When disabled, the health check will be skipped entirely and no metrics will be generated for it. This is useful for:
- Temporarily disabling problematic checks
- Maintenance periods
- Testing new health checks before enabling them
- Seasonal or conditional monitoring

## Metrics

### New Health Status Metric

```
agenda_health_status{alert_name="your_alert_name", title="Your Alert Title", description="Alert description", process="your_process"} 1
```

- `1` = Unhealthy (total > 0)
- `0` = Healthy (total = 0)

**Labels:**
- `alert_name`: The unique identifier for the health check
- `title`: Human-readable title for the alert (defaults to alert_name if not provided)
- `description`: Detailed description of what the alert monitors (empty string if not provided)
- `process`: The process name running the health checks

## Usage Example

```javascript
const { Sequelize } = require('sequelize');
const AgendaMetricsCollector = require('agenda-prom');

const sequelize = new Sequelize(/* your database config */);

const metricsCollector = new AgendaMetricsCollector(agenda, {
  processName: process.env.NAME || 'my-process',
  alloyMode: 'file',
  metricsFilePath: '/home/deploy/agenda-metrics/agenda_metrics_my-process.prom',
  updateInterval: 10000,
  
  // Health monitoring configuration
  sequelize: sequelize,
  healthMetricsDirectory: '/path/to/health/configs',
  healthCheckInterval: 30000
});
```

## Health Check Examples

### Example 1: Stuck Jobs Alert

**stuck_jobs_alert.json:**
```json
{
  "alert_name": "stuck_jobs_alert",
  "title": "Stuck Jobs Alert",
  "description": "Alert when jobs have been running for more than 2 hours",
  "enabled": true
}
```

**stuck_jobs_alert.sql:**
```sql
SELECT COUNT(*) as total 
FROM jobs 
WHERE status = 'running' 
  AND started_at < DATE_SUB(NOW(), INTERVAL 2 HOUR)
```

### Example 2: Failed Jobs Alert

**failed_jobs_alert.json:**
```json
{
  "alert_name": "failed_jobs_alert",
  "title": "Failed Jobs Alert",
  "description": "Alert when there are failed jobs in the last hour",
  "enabled": true
}
```

**failed_jobs_alert.sql:**
```sql
SELECT COUNT(*) as total 
FROM jobs 
WHERE status = 'failed' 
  AND created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
```

### Example 3: TCG Notes Not Sent

**not_sent_tcg_note.json:**
```json
{
  "alert_name": "not_sent_tcg_note",
  "title": "Unsent TCG Notes",
  "description": "Alert when TCG notes have not been sent for an extended period",
  "enabled": true
}
```

**not_sent_tcg_note.sql:**
```sql
SELECT COUNT(*) as total 
FROM tcg_notes 
WHERE sent_status IS NULL 
  OR sent_status = 'pending' 
  AND created_at < DATE_SUB(NOW(), INTERVAL 30 MINUTE)
```

## Prometheus Queries

### **Query Examples:**

1. **Check if any alerts are unhealthy:**
   ```promql
   agenda_health_status > 0
   ```

2. **Count total unhealthy alerts:**
   ```promql
   sum(agenda_health_status)
   ```

3. **Get specific alert status:**
   ```promql
   agenda_health_status{alert_name="failed_sync_operations"}
   ```

4. **Alerts for specific process:**
   ```promql
   agenda_health_status{process="ttcg-aftersales"}
   ```

## Grafana Integration

The `title` and `description` labels are perfect for Grafana alerts and notifications:

### **Grafana Alert Rules:**
```promql
# Alert when any health check fails
agenda_health_status > 0

# Alert when specific critical checks fail
agenda_health_status{alert_name=~"failed_sync_operations|pending_customers_dvla_check"} > 0
```

### **Grafana Alert Message Templates:**
```
🚨 **{{ $labels.title }}** 
Alert: {{ $labels.alert_name }}
Process: {{ $labels.process }}
Description: {{ $labels.description }}
Status: {{ if eq $value "1.0" }}UNHEALTHY{{ else }}HEALTHY{{ end }}
```

### **Benefits for Grafana:**
- **title**: Used in alert summaries and dashboard panels
- **description**: Provides context in alert notifications  
- **alert_name**: Technical identifier for grouping and filtering
- **process**: Identifies which service/process triggered the alert