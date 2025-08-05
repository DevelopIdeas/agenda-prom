# Debugging Long-Running and Stuck Jobs

## Overview

The enhanced AgendaMetricsCollector now provides comprehensive tracking of currently running jobs to help debug issues with long-running or stuck jobs.

## New Features

### 1. Individual Job Tracking
- Tracks each job instance with unique job ID
- Records start time for each running job
- Properly handles job completion, failure, and error scenarios

### 2. New Metrics

#### `agenda_job_start_time`
- **Type**: Gauge
- **Labels**: `jobName`, `jobId`, `process`
- **Description**: Start time of currently running jobs in Unix time (seconds)

#### `agenda_long_running_jobs`
- **Type**: Gauge  
- **Labels**: `jobName`, `process`, `threshold_minutes`
- **Description**: Number of jobs running longer than threshold (5, 15, 30, 60 minutes)

### 3. Diagnostic Methods

#### `getDiagnostics()`
Returns comprehensive information about currently running jobs:

```javascript
const diagnostics = metricsCollector.getDiagnostics();
console.log(diagnostics);
```

Returns:
```javascript
{
  totalRunningJobs: 3,
  runningJobs: [
    {
      jobId: "64f1234567890abcdef12345",
      jobName: "data-processing",
      process: "worker-1",
      startTime: 1692123456789,
      runningTimeMs: 1800000,
      runningTimeMinutes: 30
    }
  ],
  processName: "worker-1",
  isConnected: true,
  timestamp: 1692125256789,
  longRunningJobsCount: {
    over5min: 2,
    over15min: 1,
    over30min: 1,
    over60min: 0
  }
}
```

#### `getCurrentlyRunningJobs()`
Returns array of currently running jobs sorted by longest running first.

### 4. Automatic Monitoring

- **Stuck Job Detection**: Logs warnings for jobs running longer than 1 hour
- **Long-Running Job Metrics**: Updates every 30 seconds
- **Proper Cleanup**: Handles job failures and ensures accurate tracking

## Usage

```javascript
const metricsCollector = new AgendaMetricsCollector(agenda, {
  alloyMode: 'file',
  processName: 'my-worker'
});

// Check diagnostics
setInterval(() => {
  const diagnostics = metricsCollector.getDiagnostics();
  if (diagnostics.totalRunningJobs > 10) {
    console.warn('High number of running jobs detected:', diagnostics.totalRunningJobs);
  }
  
  // Log long-running jobs
  diagnostics.runningJobs
    .filter(job => job.runningTimeMinutes > 30)
    .forEach(job => {
      console.warn(`Long-running job: ${job.jobName} (${job.runningTimeMinutes} minutes)`);
    });
}, 60000);
```

## Debugging Workflow

1. **Monitor Metrics**: Watch `agenda_long_running_jobs` and `agenda_jobs_running` metrics
2. **Check Diagnostics**: Use `getDiagnostics()` to get detailed view of running jobs
3. **Identify Patterns**: Look for jobs that consistently run long or get stuck
4. **Investigate**: Check logs for specific job IDs that appear stuck
5. **Fix Issues**: Update job logic, add timeouts, or improve error handling

## Example Metrics Output

```
# HELP agenda_jobs_running Number of jobs currently running
# TYPE agenda_jobs_running gauge
agenda_jobs_running{jobName="data-processing",process="worker-1"} 2
agenda_jobs_running{jobName="email-sender",process="worker-1"} 1

# HELP agenda_long_running_jobs Number of jobs running longer than threshold
# TYPE agenda_long_running_jobs gauge
agenda_long_running_jobs{jobName="data-processing",process="worker-1",threshold_minutes="5"} 1
agenda_long_running_jobs{jobName="data-processing",process="worker-1",threshold_minutes="15"} 1

# HELP agenda_job_start_time Start time of currently running jobs in Unix time (seconds)
# TYPE agenda_job_start_time gauge
agenda_job_start_time{jobName="data-processing",jobId="64f1234567890abcdef12345",process="worker-1"} 1692123456
```

This enhanced tracking will help you identify and debug issues with jobs that aren't completing properly.
