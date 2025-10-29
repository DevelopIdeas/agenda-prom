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

Create JSON files in the `healthMetricsDirectory` with the following structure:

```json
{
  "alert_name": "your_alert_name",
  "sql_query": "SELECT COUNT(*) as total FROM your_table WHERE condition",
  "description": "Optional description of what this health check monitors"
}
```

**Requirements:**
- The SQL query must return a single row with a `total` field
- If `total > 0`, the health check is considered unhealthy (metric value = 1)
- If `total = 0`, the health check is considered healthy (metric value = 0)

## Metrics

### New Health Status Metric

```
agenda_health_status{alert_name="your_alert_name", process="your_process"} 1
```

- `1` = Unhealthy (total > 0)
- `0` = Healthy (total = 0)

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
```json
{
  "alert_name": "stuck_jobs_alert",
  "sql_query": "SELECT COUNT(*) as total FROM jobs WHERE status = 'running' AND started_at < DATE_SUB(NOW(), INTERVAL 2 HOUR)",
  "description": "Alert when jobs have been running for more than 2 hours"
}
```

### Example 2: Failed Jobs Alert
```json
{
  "alert_name": "failed_jobs_alert", 
  "sql_query": "SELECT COUNT(*) as total FROM jobs WHERE status = 'failed' AND created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)",
  "description": "Alert when there are failed jobs in the last hour"
}
```

### Example 3: Database Connection Issues
```json
{
  "alert_name": "db_connection_alert",
  "sql_query": "SELECT COUNT(*) as total FROM connection_errors WHERE created_at > DATE_SUB(NOW(), INTERVAL 5 MINUTE)",
  "description": "Alert when there are database connection errors in the last 5 minutes"
}
```