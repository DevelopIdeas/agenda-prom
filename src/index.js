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
      labelNames: ['jobName', 'process']
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
    
    // Register metrics
    this.register.registerMetric(this.jobsProcessedCounter);
    this.register.registerMetric(this.jobDurationHistogram);
    this.register.registerMetric(this.jobQueueGauge);
    this.register.registerMetric(this.dbConnectionGauge);
    this.register.registerMetric(this.runningJobsGauge);
    this.register.registerMetric(this.lastJobStartGauge);
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
      
      // Update last job start timestamp
      const currentTimestamp = Math.floor(Date.now() / 1000);
      this.lastJobStartGauge.set({ process: this.processName }, currentTimestamp);
      
      // Increment running jobs counter
      this.runningJobsGauge.inc({ jobName, process: this.processName });
      
      if (this.alloyMode === 'file') {
        this.writeMetricsToFile();
      }
    });
    
    this.agenda.on('complete', (job) => {
      const jobName = job.attrs.name;
      
      // Decrement running jobs counter
      this.runningJobsGauge.dec({ jobName, process: this.processName });
      
      if (this.alloyMode === 'file') {
        this.writeMetricsToFile();
      }
    });
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
      
      try {
        // Execute the original job function
        await jobFn(job, done);
        
        // Update metrics for successful job
        this.jobsProcessedCounter.inc({ jobName, status: 'success', process: this.processName });
      } catch (error) {
        // Update metrics for failed job
        this.jobsProcessedCounter.inc({ jobName, status: 'failure', process: this.processName });
        
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
   * Get the Prometheus registry for direct integration with Alloy
   */
  getRegistry() {
    return this.register;
  }
  
  cleanup() {
    // Clear interval
    if (this.updateIntervalId) {
      clearInterval(this.updateIntervalId);
    }
    
    // Remove event listeners if possible
    if (this.agenda.removeAllListeners) {
      this.agenda.removeAllListeners('ready');
      this.agenda.removeAllListeners('error');
      this.agenda.removeAllListeners('start');
      this.agenda.removeAllListeners('complete');
    }
  }
}

module.exports = AgendaMetricsCollector;