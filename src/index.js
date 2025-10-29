// agendaMetricsForAlloy.js
const client = require('prom-client');
const fs = require('fs');
const path = require('path');

/**
 * AgendaMetricsCollector for Alloy integration
 * 
 * This collector writes metrics to a file that can be read by Alloy's
 * Prometheus integration or uses in-memory registry that can be merged 
 * with Alloy's registry.
 */
class AgendaMetricsCollector {
  constructor(agenda, options = {}) {
    // Configuration
    this.alloyMode = options.alloyMode || 'file'; // 'file' or 'registry'
    this.metricsFilePath = options.metricsFilePath || path.join(process.cwd(), 'agenda_metrics.prom');
    this.updateInterval = options.updateInterval || 5000; // 5 seconds
    this.processName = options.processName || process.env.NAME || 'default-process';
    
    // Health monitoring configuration
    this.sequelize = options.sequelize || null;
    this.healthMetricsDirectory = options.healthMetricsDirectory || null;
    this.healthCheckInterval = options.healthCheckInterval || 30000; // 30 seconds
    
    const redundantPrefix = 'data-warehouse-'
    const redundantSuffix = '-agenda'
    if (this.processName && this.processName !== `${redundantPrefix}agenda` && this.processName.startsWith(redundantPrefix)) {
      this.processName = this.processName.replace(redundantPrefix,'')
    }
    if (this.processName.endsWith(redundantSuffix)) {
      this.processName = this.processName.replace(redundantSuffix,'')
    }
    this.agenda = agenda;
    
    // Create a Registry
    this.register = new client.Registry();
    
    // Track currently running jobs with their start times
    this.runningJobs = new Map(); // jobId -> { jobName, startTime, process }
    
    // Initialize metrics
    this.initializeMetrics();
    
    // Track connection status
    this.isConnected = false;
    
    // Bind methods to ensure correct `this` context
    // this.updateQueueMetrics = this.updateQueueMetrics.bind(this);
    this.writeMetricsToFile = this.writeMetricsToFile.bind(this);
    this.setupEventListeners = this.setupEventListeners.bind(this);
    this.withMetrics = this.withMetrics.bind(this);
    this.cleanup = this.cleanup.bind(this);
    this.getRegistry = this.getRegistry.bind(this);
    
    // Start metrics collection
    this.setupEventListeners();
    // this.updateIntervalId = setInterval(async () => {
    //   await this.updateQueueMetrics();
      
    //   // If using file mode, write metrics to file
    //   if (this.alloyMode === 'file') {
    //     this.writeMetricsToFile();
    //   }
    // }, this.updateInterval);
    
    // Set initial last job start time
    this.lastJobStartGauge.set({ process: this.processName }, Math.floor(Date.now() / 1000));
    
    // Initial metrics write if using file mode
    if (this.alloyMode === 'file') {
      this.writeMetricsToFile();
    }
    
    // Start health check monitoring if configured
    if (this.sequelize && this.healthMetricsDirectory) {
      console.log(`Health monitoring enabled - directory: ${this.healthMetricsDirectory}, interval: ${this.healthCheckInterval}ms`);
      
      // Execute initial health checks
      this.executeAllHealthChecks().catch(err => {
        console.error('Error executing initial health checks:', err);
      });
      
      // Set up periodic health checks
      this.healthCheckIntervalId = setInterval(async () => {
        try {
          await this.executeAllHealthChecks();
        } catch (err) {
          console.error('Error executing periodic health checks:', err);
        }
      }, this.healthCheckInterval);
    }
    
    console.log(`Agenda metrics collector initialized in ${this.alloyMode} mode`);
    if (this.alloyMode === 'file') {
      console.log(`Writing metrics to: ${this.metricsFilePath}`);
    }
  }
  
  initializeMetrics() {
    // Create custom metrics
    this.jobsProcessedCounter = new client.Counter({
      name: 'agenda_jobs_processed_total',
      help: 'Total number of processed jobs',
      labelNames: ['jobName', 'status', 'process']
    });
    
    this.jobDurationHistogram = new client.Histogram({
      name: 'agenda_job_duration_seconds',
      help: 'Job execution time in seconds',
      labelNames: ['jobName', 'process'],
      buckets: [10, 60, 300, 600],
    });
    
    this.jobQueueGauge = new client.Gauge({
      name: 'agenda_jobs_in_queue',
      help: 'Number of jobs waiting in queue',
      labelNames: ['jobName', 'process']
    });
    
    this.dbConnectionGauge = new client.Gauge({
      name: 'agenda_db_connected',
      help: 'Database connection status (1 = connected, 0 = disconnected)',
      labelNames: ['process']
    });
    
    this.runningJobsGauge = new client.Gauge({
      name: 'agenda_jobs_running',
      help: 'Number of jobs currently running',
      labelNames: ['jobName', 'process']
    });
    
    this.lastJobStartGauge = new client.Gauge({
      name: 'agenda_last_job_start_timestamp',
      help: 'Timestamp of the last job started in Unix time (seconds)',
      labelNames: ['process']
    });
    
    this.jobStartTimeGauge = new client.Gauge({
      name: 'agenda_job_start_time',
      help: 'Start time of currently running jobs in Unix time (seconds)',
      labelNames: ['jobName', 'jobId', 'process']
    });
    
    this.longRunningJobsGauge = new client.Gauge({
      name: 'agenda_long_running_jobs',
      help: 'Number of jobs running longer than threshold',
      labelNames: ['jobName', 'process', 'threshold_minutes']
    });
    
    this.healthStatusGauge = new client.Gauge({
      name: 'agenda_health_status',
      help: 'Health status of monitored alerts (1 = unhealthy, 0 = healthy)',
      labelNames: ['alert_name', 'process']
    });
    
    // Register metrics
    this.register.registerMetric(this.jobsProcessedCounter);
    this.register.registerMetric(this.jobDurationHistogram);
    this.register.registerMetric(this.jobQueueGauge);
    this.register.registerMetric(this.dbConnectionGauge);
    this.register.registerMetric(this.runningJobsGauge);
    this.register.registerMetric(this.lastJobStartGauge);
    this.register.registerMetric(this.jobStartTimeGauge);
    this.register.registerMetric(this.longRunningJobsGauge);
    this.register.registerMetric(this.healthStatusGauge);
  }
  
  /**
   * Load health check configurations from JSON files in the specified directory
   */
  loadHealthCheckConfigs() {
    if (!this.healthMetricsDirectory) {
      return [];
    }
    
    try {
      if (!fs.existsSync(this.healthMetricsDirectory)) {
        console.warn(`Health metrics directory not found: ${this.healthMetricsDirectory}`);
        return [];
      }
      
      const files = fs.readdirSync(this.healthMetricsDirectory).filter(file => file.endsWith('.json'));
      const configs = [];
      
      for (const file of files) {
        try {
          const filePath = path.join(this.healthMetricsDirectory, file);
          const config = JSON.parse(fs.readFileSync(filePath, 'utf8'));
          
          // Validate required fields
          if (!config.alert_name || !config.sql_query) {
            console.warn(`Invalid health check config in ${file}: missing alert_name or sql_query`);
            continue;
          }
          
          configs.push({
            ...config,
            file: file
          });
        } catch (err) {
          console.error(`Error loading health check config from ${file}:`, err);
        }
      }
      
      console.log(`Loaded ${configs.length} health check configurations`);
      return configs;
    } catch (err) {
      console.error('Error loading health check configurations:', err);
      return [];
    }
  }
  
  /**
   * Execute a single health check using Sequelize
   */
  async executeHealthCheck(config) {
    if (!this.sequelize) {
      console.warn('Sequelize instance not provided, skipping health check:', config.alert_name);
      return;
    }
    
    try {
      const [results] = await this.sequelize.query(config.sql_query);
      
      if (!results || results.length === 0) {
        console.warn(`Health check query returned no results for ${config.alert_name}`);
        this.healthStatusGauge.set({ alert_name: config.alert_name, process: this.processName }, 0);
        return;
      }
      
      const result = results[0];
      const total = result.total || result.Total || result.TOTAL || 0;
      
      // Set health status: 1 if unhealthy (total > 0), 0 if healthy
      const isUnhealthy = total > 0 ? 1 : 0;
      this.healthStatusGauge.set({ alert_name: config.alert_name, process: this.processName }, isUnhealthy);
      
      if (isUnhealthy) {
        console.warn(`Health check UNHEALTHY: ${config.alert_name} - total: ${total}`);
      } else {
        console.log(`Health check OK: ${config.alert_name} - total: ${total}`);
      }
    } catch (err) {
      console.error(`Error executing health check for ${config.alert_name}:`, err);
      // Set to healthy (0) on error to avoid false alarms
      this.healthStatusGauge.set({ alert_name: config.alert_name, process: this.processName }, 0);
    }
  }
  
  /**
   * Execute all health checks
   */
  async executeAllHealthChecks() {
    const configs = this.loadHealthCheckConfigs();
    
    for (const config of configs) {
      await this.executeHealthCheck(config);
    }
    
    // Write updated metrics to file if in file mode
    if (this.alloyMode === 'file') {
      this.writeMetricsToFile();
    }
  }
  
  setupEventListeners() {
    // Monitor database connection
    this.agenda.on('ready', () => {
      console.log('Agenda metrics: connected to MongoDB');
      this.isConnected = true;
      this.dbConnectionGauge.set({ process: this.processName }, 1);
      
      if (this.alloyMode === 'file') {
        this.writeMetricsToFile();
      }
    });
    
    this.agenda.on('error', (err) => {
      console.error('Agenda metrics: MongoDB connection error:', err);
      this.isConnected = false;
      this.dbConnectionGauge.set({ process: this.processName }, 0);
      
      if (this.alloyMode === 'file') {
        this.writeMetricsToFile();
      }
    });
    
    // Monitor job starts and completions
    this.agenda.on('start', (job) => {
      const jobName = job.attrs.name;
      const jobId = job.attrs._id.toString();
      const startTime = Date.now();
      
      console.log(`Job started: ${jobName} (ID: ${jobId})`);
      
      // Track the running job
      this.runningJobs.set(jobId, {
        jobName,
        startTime,
        process: this.processName
      });
      
      // Update last job start timestamp
      const currentTimestamp = Math.floor(startTime / 1000);
      this.lastJobStartGauge.set({ process: this.processName }, currentTimestamp);
      
      // Set job start time metric
      this.jobStartTimeGauge.set({ 
        jobName, 
        jobId, 
        process: this.processName 
      }, currentTimestamp);
      
      // Increment running jobs counter
      this.runningJobsGauge.inc({ jobName, process: this.processName });
      
      // Update long-running job metrics
      this.updateLongRunningJobMetrics();
      
      if (this.alloyMode === 'file') {
        this.writeMetricsToFile();
      }
    });
    
    this.agenda.on('complete', (job) => {
      this.handleJobCompletion(job, 'complete');
    });
    
    this.agenda.on('fail', (err, job) => {
      console.error(`Job failed: ${job.attrs.name} (ID: ${job.attrs._id.toString()})`, err);
      this.handleJobCompletion(job, 'fail');
    });
    
    // Set up periodic check for long-running jobs
    this.longRunningJobCheckInterval = setInterval(() => {
      this.updateLongRunningJobMetrics();
      this.checkForStuckJobs();
    }, 30000); // Check every 30 seconds
  }
  
  handleJobCompletion(job, status) {
    const jobName = job.attrs.name;
    const jobId = job.attrs._id.toString();
    
    console.log(`Job ${status}: ${jobName} (ID: ${jobId})`);
    
    // Remove from running jobs tracking
    const runningJob = this.runningJobs.get(jobId);
    if (runningJob) {
      this.runningJobs.delete(jobId);
      
      // Clear job start time metric
      this.jobStartTimeGauge.remove({ 
        jobName, 
        jobId, 
        process: this.processName 
      });
      
      // Decrement running jobs counter
      this.runningJobsGauge.dec({ jobName, process: this.processName });
      
      // Update long-running job metrics
      this.updateLongRunningJobMetrics();
      
      if (this.alloyMode === 'file') {
        this.writeMetricsToFile();
      }
    } else {
      console.warn(`Job completion event for unknown job: ${jobName} (ID: ${jobId})`);
    }
  }
  
  updateLongRunningJobMetrics() {
    const now = Date.now();
    const thresholds = [5, 15, 30, 60]; // minutes
    
    // Reset all long-running job counters
    for (const threshold of thresholds) {
      this.longRunningJobsGauge.reset();
    }
    
    // Count long-running jobs by threshold
    const jobCounts = new Map();
    
    for (const [jobId, jobInfo] of this.runningJobs) {
      const runningTimeMinutes = (now - jobInfo.startTime) / (1000 * 60);
      
      for (const threshold of thresholds) {
        if (runningTimeMinutes > threshold) {
          const key = `${jobInfo.jobName}_${threshold}`;
          jobCounts.set(key, (jobCounts.get(key) || 0) + 1);
        }
      }
    }
    
    // Update metrics
    for (const [key, count] of jobCounts) {
      const [jobName, threshold] = key.split('_');
      this.longRunningJobsGauge.set({ 
        jobName, 
        process: this.processName, 
        threshold_minutes: threshold 
      }, count);
    }
  }
  
  checkForStuckJobs() {
    const now = Date.now();
    const stuckThreshold = 60 * 60 * 1000; // 1 hour
    
    for (const [jobId, jobInfo] of this.runningJobs) {
      const runningTime = now - jobInfo.startTime;
      if (runningTime > stuckThreshold) {
        console.warn(`Potentially stuck job detected: ${jobInfo.jobName} (ID: ${jobId}) running for ${Math.round(runningTime / (1000 * 60))} minutes`);
      }
    }
  }
  
  getCurrentlyRunningJobs() {
    const now = Date.now();
    const jobs = [];
    
    for (const [jobId, jobInfo] of this.runningJobs) {
      jobs.push({
        jobId,
        jobName: jobInfo.jobName,
        process: jobInfo.process,
        startTime: jobInfo.startTime,
        runningTimeMs: now - jobInfo.startTime,
        runningTimeMinutes: Math.round((now - jobInfo.startTime) / (1000 * 60))
      });
    }
    
    return jobs.sort((a, b) => b.runningTimeMs - a.runningTimeMs); // Sort by longest running first
  }
  
  async updateQueueMetrics() {
    if (!this.isConnected) return;
    
    try {
      // Get all job names
      const jobs = await this.agenda.jobs()
      
      // For each job name, count jobs and update metrics
      for (const job of jobs) {
        const jobName = job.attrs.name;
        const js = await this.agenda.jobs({ name: jobName, nextRunAt: { $ne: null } })
        if (!js || js.length === 0) {
          continue;
        }
        const j = js[0];
        console.log(jobName,'jobName')
        console.log(j,'j')
        const count = j.count();
        this.jobQueueGauge.set({ jobName, process: this.processName }, count);
      }
    } catch (err) {
      console.error('Agenda metrics: Error updating queue metrics:', err);
      this.dbConnectionGauge.set({ process: this.processName }, 0);
    }
  }
  
  async writeMetricsToFile() {
    try {
      const metrics = await this.register.metrics();
      fs.writeFileSync(this.metricsFilePath, metrics);
    } catch (err) {
      console.error('Failed to write metrics to file:', err);
    }
  }
  
  withMetrics(jobName, jobFn) {
    return async (job, done) => {
      const startTime = Date.now();
      const jobId = job.attrs._id.toString();
      
      try {
        // Execute the original job function
        await jobFn(job, done);
        
        // Update metrics for successful job
        this.jobsProcessedCounter.inc({ jobName, status: 'success', process: this.processName });
      } catch (error) {
        // Update metrics for failed job
        this.jobsProcessedCounter.inc({ jobName, status: 'failure', process: this.processName });
        
        // Ensure job is removed from running jobs tracking on error
        if (this.runningJobs.has(jobId)) {
          console.warn(`Cleaning up failed job from running jobs tracking: ${jobName} (ID: ${jobId})`);
          this.runningJobs.delete(jobId);
          this.jobStartTimeGauge.remove({ jobName, jobId, process: this.processName });
          this.runningJobsGauge.dec({ jobName, process: this.processName });
        }
        
        // Re-throw the error so Agenda can handle it
        throw error;
      } finally {
        // Record job duration
        const duration = (Date.now() - startTime) / 1000;
        this.jobDurationHistogram.observe({ jobName, process: this.processName }, duration);
        
        // Write updated metrics to file if in file mode
        if (this.alloyMode === 'file') {
          this.writeMetricsToFile();
        }
      }
    };
  }

  defineJob(jobName, jobOpts, jobFn) {
    // Define the job with metrics
    this.agenda.define(jobName, jobOpts, this.withMetrics(jobName, jobFn));
    
    // Return the defined job for further use
    // return this.agenda.job(jobName);
  }
  
  /**
   * Get diagnostic information about currently running jobs
   */
  getDiagnostics() {
    const runningJobs = this.getCurrentlyRunningJobs();
    const now = Date.now();
    
    return {
      totalRunningJobs: this.runningJobs.size,
      runningJobs,
      processName: this.processName,
      isConnected: this.isConnected,
      timestamp: now,
      longRunningJobsCount: {
        over5min: runningJobs.filter(j => j.runningTimeMinutes > 5).length,
        over15min: runningJobs.filter(j => j.runningTimeMinutes > 15).length,
        over30min: runningJobs.filter(j => j.runningTimeMinutes > 30).length,
        over60min: runningJobs.filter(j => j.runningTimeMinutes > 60).length,
      }
    };
  }
  
  /**
   * Get the Prometheus registry for direct integration with Alloy
   */
  getRegistry() {
    return this.register;
  }
  
  cleanup() {
    // Clear intervals
    if (this.updateIntervalId) {
      clearInterval(this.updateIntervalId);
    }
    if (this.longRunningJobCheckInterval) {
      clearInterval(this.longRunningJobCheckInterval);
    }
    if (this.healthCheckIntervalId) {
      clearInterval(this.healthCheckIntervalId);
    }
    
    // Clear running jobs tracking
    this.runningJobs.clear();
    
    // Remove event listeners if possible
    if (this.agenda.removeAllListeners) {
      this.agenda.removeAllListeners('ready');
      this.agenda.removeAllListeners('error');
      this.agenda.removeAllListeners('start');
      this.agenda.removeAllListeners('complete');
      this.agenda.removeAllListeners('fail');
    }
  }
}

module.exports = AgendaMetricsCollector;