// agendaMetricsForAlloy.js
const client = require('prom-client');
const fs = require('fs');
const path = require('path');

const SYSTEM_HEALTH_CHECK_TIMEOUT_MS = 5000;
const SYSTEM_JOB_TIMEOUT_MINUTES = 60;
const SYSTEM_LONG_RUNNING_JOB_MINUTES = 30;

/**
 * AgendaMetricsCollector for Alloy integration
 * 
 * This collector writes metrics to a file that can be read by Alloy's
 * Prometheus integration or uses in-memory registry that can be merged 
 * with Alloy's registry.
 *
 * By default also runs a system health check every 5 minutes (DB connections,
 * Agenda/Mongo, long-running / stuck jobs). Configure via options.healthCheck;
 * set HEALTH_CHECK_AUTO_RESTART=1 (or healthCheck.autoRestartOnStuck) to exit
 * when no job has run for > 60 minutes.
 */
class AgendaMetricsCollector {
  constructor(agenda, options = {}) {
    // Configuration
    this.alloyMode = options.alloyMode || 'file'; // 'file' or 'registry'
    this.metricsFilePath = options.metricsFilePath || path.join(process.cwd(), 'agenda_metrics.prom');
    this.updateInterval = options.updateInterval || 5000; // 5 seconds
    this.processName = options.processName || process.env.NAME || 'default-process';
    
    // SQL alert health monitoring (JSON/SQL configs live next to the metrics file)
    this.sequelize = options.sequelize || null;
    this.healthMetricsDirectory = path.dirname(this.metricsFilePath);
    this.healthCheckInterval = options.healthCheckInterval || 30000; // 30 seconds
    this.extraMetrics = Array.isArray(options.extraMetrics) ? options.extraMetrics : [];

    // System health check (DB auth, Agenda/Mongo, stuck jobs) — on by default
    // Pass options.healthCheck = false to disable, or an object:
    // { dbs, intervalMinutes, autoRestartOnStuck, enabled }
    // Logger: options.logger (top-level)
    const healthCheck = options.healthCheck === false
      ? { enabled: false }
      : (options.healthCheck && typeof options.healthCheck === 'object' ? options.healthCheck : {});
    this.dbs = healthCheck.dbs && typeof healthCheck.dbs === 'object' ? healthCheck.dbs : {};
    this.logger = this._normalizeLogger(options.logger || global.__logger);
    this.systemHealthCheckEnabled = healthCheck.enabled !== false;
    this.systemHealthCheckIntervalMinutes = healthCheck.intervalMinutes || 5;
    this.autoRestartOnStuck = healthCheck.autoRestartOnStuck != null
      ? !!healthCheck.autoRestartOnStuck
      : process.env.HEALTH_CHECK_AUTO_RESTART === '1';
    this._systemHealth = {
      dbs: {},
      mongo: { status: 'unknown', lastCheck: null, error: null },
      longRunningJobs: { count: 0, jobs: [], lastCheck: null, error: null },
      jobStatuses: {},
      latestJobRun: null,
      lastFullCheck: null,
    };
    
    const redundantPrefix = options.redundantPrefix != null ? options.redundantPrefix : 'data-warehouse-';
    const redundantSuffix = options.redundantSuffix != null ? options.redundantSuffix : '';
    if (this.processName && this.processName !== `${redundantPrefix}agenda` && this.processName.startsWith(redundantPrefix)) {
      this.processName = this.processName.replace(redundantPrefix,'')
    }
    if (redundantSuffix && this.processName.endsWith(redundantSuffix)) {
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
    this.registerMetric = this.registerMetric.bind(this);
    this.performSystemHealthCheck = this.performSystemHealthCheck.bind(this);
    
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
    
    // Start SQL alert health checks if sequelize is provided
    // (configs are loaded from the same directory as metricsFilePath)
    if (this.sequelize) {
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

    // Start system health check by default (DB / Agenda / stuck jobs)
    if (this.systemHealthCheckEnabled) {
      this.startSystemHealthCheck();
    }
    
    console.log(`Agenda metrics collector initialized in ${this.alloyMode} mode`);
    if (this.alloyMode === 'file') {
      console.log(`Writing metrics to: ${this.metricsFilePath}`);
    }
  }

  _normalizeLogger(logger) {
    if (logger && typeof logger.info === 'function') {
      return {
        debug: typeof logger.debug === 'function' ? logger.debug.bind(logger) : logger.info.bind(logger),
        info: logger.info.bind(logger),
        warn: typeof logger.warn === 'function' ? logger.warn.bind(logger) : logger.info.bind(logger),
        error: typeof logger.error === 'function' ? logger.error.bind(logger) : logger.info.bind(logger),
      };
    }
    return {
      debug: (...args) => console.debug(...args),
      info: (...args) => console.log(...args),
      warn: (...args) => console.warn(...args),
      error: (...args) => console.error(...args),
    };
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
      labelNames: ['alert_name', 'title', 'description', 'process']
    });

    // System health gauges (same names as data-warehouse health-check util)
    this.systemHealthStatusGauge = new client.Gauge({
      name: 'system_health_status',
      help: 'System health status (1=healthy, 0=unhealthy)',
      labelNames: ['process'],
    });
    this.systemUnresponsiveGauge = new client.Gauge({
      name: 'system_unresponsive_status',
      help: 'System unresponsive status (1=unresponsive, 0=responsive)',
      labelNames: ['process'],
    });
    this.systemLongRunningJobsGauge = new client.Gauge({
      name: 'long_running_jobs_count',
      help: 'Number of long-running jobs detected',
      labelNames: ['process'],
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
    this.register.registerMetric(this.systemHealthStatusGauge);
    this.register.registerMetric(this.systemUnresponsiveGauge);
    this.register.registerMetric(this.systemLongRunningJobsGauge);

    const metricLabels = { process: this.processName };
    this.systemHealthStatusGauge.set(metricLabels, 1);
    this.systemUnresponsiveGauge.set(metricLabels, 0);
    this.systemLongRunningJobsGauge.set(metricLabels, 0);

    // Register caller-provided metrics after built-ins.
    for (const metric of this.extraMetrics) {
      this.registerMetric(metric);
    }
  }

  registerMetric(metric) {
    if (!metric || typeof metric !== 'object') {
      return false;
    }

    try {
      this.register.registerMetric(metric);
      return true;
    } catch (err) {
      const metricName = metric.name || 'unknown_metric';
      console.warn(`Failed to register extra metric ${metricName}:`, err.message || err);
      return false;
    }
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
          if (!config.alert_name) {
            console.warn(`Invalid health check config in ${file}: missing alert_name`);
            continue;
          }
          
          // Load SQL query from corresponding .sql file
          const baseName = path.basename(file, '.json');
          const sqlFilePath = path.join(this.healthMetricsDirectory, `${baseName}.sql`);
          
          if (!fs.existsSync(sqlFilePath)) {
            console.warn(`SQL file not found for health check ${file}: ${sqlFilePath}`);
            continue;
          }
          
          const sqlQuery = fs.readFileSync(sqlFilePath, 'utf8').trim();
          if (!sqlQuery) {
            console.warn(`Empty SQL query in ${sqlFilePath}`);
            continue;
          }
          
          configs.push({
            ...config,
            sql_query: sqlQuery,
            file: file,
            sql_file: `${baseName}.sql`,
            enabled: config.enabled !== undefined ? config.enabled : true
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
        this.healthStatusGauge.set({ 
          alert_name: config.alert_name, 
          title: config.title || config.alert_name,
          description: config.description || '',
          process: this.processName 
        }, 0);
        return;
      }
      
      const result = results[0];
      const total = result.total || result.Total || result.TOTAL || 0;
      
      // Set health status: 1 if unhealthy (total > 0), 0 if healthy
      const isUnhealthy = total > 0 ? 1 : 0;
      this.healthStatusGauge.set({ 
        alert_name: config.alert_name, 
        title: config.title || config.alert_name,
        description: config.description || '',
        process: this.processName 
      }, isUnhealthy);
      
      if (isUnhealthy) {
        console.warn(`Health check UNHEALTHY: ${config.alert_name} - total: ${total}`);
      } else {
        console.log(`Health check OK: ${config.alert_name} - total: ${total}`);
      }
    } catch (err) {
      console.error(`Error executing health check for ${config.alert_name}:`, err);
      // Set to healthy (0) on error to avoid false alarms
      this.healthStatusGauge.set({ 
        alert_name: config.alert_name, 
        title: config.title || config.alert_name,
        description: config.description || '',
        process: this.processName 
      }, 0);
    }
  }
  
  /**
   * Execute all health checks
   */
  async executeAllHealthChecks() {
    const configs = this.loadHealthCheckConfigs();
    
    for (const config of configs) {
      if (config.enabled) {
        await this.executeHealthCheck(config);
      } else {
        console.log(`Health check disabled, skipping: ${config.alert_name}`);
      }
    }
    
    // Write updated metrics to file if in file mode
    if (this.alloyMode === 'file') {
      this.writeMetricsToFile();
    }
  }

  async _runCheckWithTimeout(name, checkFn, timeout = SYSTEM_HEALTH_CHECK_TIMEOUT_MS) {
    const startTime = Date.now();

    this.logger.debug('Running check with timeout', { name, timeout });

    return Promise.race([
      checkFn()
        .then((result) => ({
          success: true,
          error: null,
          result,
          duration: Date.now() - startTime,
        }))
        .catch((err) => ({
          success: false,
          error: err,
          duration: Date.now() - startTime,
        })),
      new Promise((resolve) =>
        setTimeout(
          () => resolve({
            success: false,
            error: new Error(`${name} check timed out after ${timeout}ms`),
            isTimeout: true,
            duration: timeout,
          }),
          timeout
        )
      ),
    ]);
  }

  async _checkDb(db, name) {
    if (!db) throw new Error(`db ${name} not initialized`);
    await db.authenticate();
    await db.query('SELECT 1', { type: 'SELECT' });
    return { connected: true };
  }

  async _checkMongo() {
    if (!this.agenda) throw new Error('Agenda not initialized');
    if (this.agenda._collection && this.agenda._db) {
      const count = await this.agenda._collection.countDocuments();
      return { connected: true, scheduledJobs: count };
    }
    throw new Error('Agenda connection not ready');
  }

  async _checkLongRunningJobs(thresholdMinutes = SYSTEM_LONG_RUNNING_JOB_MINUTES) {
    if (!this.agenda || !this.agenda._collection) {
      throw new Error('Agenda not initialized');
    }

    const thresholdMs = thresholdMinutes * 60 * 1000;
    const now = new Date();
    const thresholdTime = new Date(now.getTime() - thresholdMs);

    const longRunningJobs = await this.agenda._collection
      .find({
        lockedAt: { $exists: true, $ne: null },
        lastRunAt: { $exists: true, $ne: null, $lt: thresholdTime },
        $or: [
          { lastFinishedAt: { $exists: false } },
          { lastFinishedAt: null },
          { $expr: { $gt: ['$lastRunAt', '$lastFinishedAt'] } },
        ],
      })
      .toArray();

    return {
      count: longRunningJobs.length,
      jobs: longRunningJobs.map((job) => ({
        name: job.name,
        startedAt: job.lastRunAt,
        durationMin: Math.round((now - job.lastRunAt) / 60000),
        lockedAt: job.lockedAt,
      })),
      threshold: `${thresholdMinutes} minutes`,
    };
  }

  async _getJobStatuses() {
    if (!this.agenda) {
      throw new Error('Agenda not initialized');
    }

    const jobs = await this.agenda.jobs({});
    const jobStatuses = {};
    let latestRunTime = null;

    for (const job of jobs) {
      const name = job.attrs.name;
      const lastRunAt = job.attrs.lastRunAt;
      const isRunning = await job.isRunning();

      jobStatuses[name] = {
        name,
        is_running: isRunning,
        last_run_at: lastRunAt,
        locked_at: job.attrs.lockedAt,
        last_finished_at: job.attrs.lastFinishedAt,
        next_run_at: job.attrs.nextRunAt,
      };

      if (lastRunAt) {
        const runTime = new Date(lastRunAt).getTime();
        if (!latestRunTime || runTime > latestRunTime.getTime()) {
          latestRunTime = new Date(lastRunAt);
        }
      }
    }

    return {
      jobs: jobStatuses,
      latestRunTime,
      totalJobs: jobs.length,
    };
  }

  /**
   * Full system health check: Sequelize DBs, Agenda/Mongo, job statuses, stuck detection.
   * Compatible with the former startHealthCheckInterval() util in data-warehouse projects.
   */
  async performSystemHealthCheck() {
    this.logger.debug('Performing system health check', { process: this.processName });
    const startTime = Date.now();
    const checkResults = { dbs: {} };
    const metricLabels = { process: this.processName };
    const health = this._systemHealth;
    const nowIso = () => new Date().toISOString();

    try {
      for (const [name, db] of Object.entries(this.dbs)) {
        if (!db) continue;

        const result = await this._runCheckWithTimeout(
          `db:${name}`,
          () => this._checkDb(db, name)
        );
        health.dbs[name] = {
          status: result.success ? 'healthy' : 'unhealthy',
          lastCheck: nowIso(),
          error: result.error ? result.error.message : null,
          duration: result.duration,
        };
        checkResults.dbs[name] = health.dbs[name];
      }

      if (this.agenda) {
        const result = await this._runCheckWithTimeout('MongoDB/Agenda', () => this._checkMongo());
        health.mongo = {
          status: result.success ? 'healthy' : 'unhealthy',
          lastCheck: nowIso(),
          error: result.error ? result.error.message : null,
          duration: result.duration,
          scheduledJobs: result.result?.scheduledJobs || null,
        };
        checkResults.mongo = health.mongo;

        const statusResult = await this._runCheckWithTimeout('jobStatuses', () => this._getJobStatuses());
        if (statusResult.success) {
          health.jobStatuses = statusResult.result.jobs;
          health.latestJobRun = statusResult.result.latestRunTime;
          checkResults.jobStatuses = {
            total_jobs: statusResult.result.totalJobs,
            jobs: statusResult.result.jobs,
            latest_run: statusResult.result.latestRunTime
              ? statusResult.result.latestRunTime.toISOString()
              : null,
          };
        } else {
          this.logger.warn('Could not fetch job statuses', { error: statusResult.error?.message });
        }

        const longResult = await this._runCheckWithTimeout(
          'longRunningJobs',
          () => this._checkLongRunningJobs(SYSTEM_LONG_RUNNING_JOB_MINUTES)
        );
        health.longRunningJobs = {
          count: longResult.success ? longResult.result.count : 0,
          jobs: longResult.success ? longResult.result.jobs : [],
          lastCheck: nowIso(),
          error: longResult.error ? longResult.error.message : null,
          threshold: longResult.success ? longResult.result.threshold : null,
          duration: longResult.duration,
        };
        checkResults.longRunningJobs = health.longRunningJobs;
      }

      let systemUnresponsive = false;
      if (health.latestJobRun) {
        const elapsed = Math.floor((Date.now() - health.latestJobRun.getTime()) / 60000);
        checkResults.jobTimeout = {
          latestRunMinutesAgo: elapsed,
          threshold: SYSTEM_JOB_TIMEOUT_MINUTES,
          isStuck: elapsed > SYSTEM_JOB_TIMEOUT_MINUTES,
        };

        if (elapsed > SYSTEM_JOB_TIMEOUT_MINUTES) {
          systemUnresponsive = true;
          this.logger.error('CRITICAL: No jobs executed for > 60 minutes', {
            elapsed,
            latestRun: health.latestJobRun.toISOString(),
            jobStatuses: health.jobStatuses,
          });
        }
      }

      const dbChecksHealthy = Object.values(health.dbs).every((status) => {
        return status.status === 'healthy' || status.status === 'unknown';
      });
      const mongoHealthy = !health.mongo?.status
        || health.mongo.status === 'healthy'
        || health.mongo.status === 'unknown';
      const coreChecksHealthy = dbChecksHealthy && mongoHealthy;
      const longRunningJobsHealthy = !health.longRunningJobs?.error && health.longRunningJobs?.count === 0;
      const allHealthy = coreChecksHealthy && longRunningJobsHealthy;

      const summary = {
        timestamp: nowIso(),
        healthy: allHealthy && !systemUnresponsive,
        systemUnresponsive,
        duration: Date.now() - startTime,
        checks: checkResults,
      };

      health.lastFullCheck = summary;

      this.systemUnresponsiveGauge.set(metricLabels, systemUnresponsive ? 1 : 0);
      this.systemLongRunningJobsGauge.set(metricLabels, health.longRunningJobs?.count || 0);

      if (allHealthy && !systemUnresponsive) {
        this.logger.debug('Health Check: All systems operational', summary);
        this.systemHealthStatusGauge.set(metricLabels, 1);
      } else if (systemUnresponsive) {
        this.logger.error('Health Check: System unresponsive - no jobs running', summary);
        this.systemHealthStatusGauge.set(metricLabels, 0);
      } else {
        this.logger.warn('Health Check: Issues detected', summary);
        this.systemHealthStatusGauge.set(metricLabels, 0);
      }

      if (this.alloyMode === 'file') {
        try {
          await this.writeMetricsToFile();
        } catch (err) {
          this.logger.warn('Failed to flush metrics file after health update', {
            error: err?.message || err,
          });
        }
      }

      Object.entries(health.dbs).forEach(([name, status]) => {
        if (status?.status === 'unhealthy' && status.error) {
          this.logger.error(`Health Check - db ${name} unhealthy`, {
            error: status.error,
            duration: status.duration,
          });
        }
      });

      if (health.mongo?.status === 'unhealthy' && health.mongo.error) {
        this.logger.error('Health Check - mongo unhealthy', {
          error: health.mongo.error,
          duration: health.mongo.duration,
        });
      }

      if (health.longRunningJobs.count > 0) {
        this.logger.warn('Health Check: Long-running jobs detected', {
          count: health.longRunningJobs.count,
          jobs: health.longRunningJobs.jobs,
        });
      }

      return summary;
    } catch (err) {
      this.logger.error('Health Check fatal error', err);
      return {
        timestamp: nowIso(),
        healthy: false,
        error: err.message,
        duration: Date.now() - startTime,
      };
    }
  }

  /**
   * Start periodic system health checks. Called automatically unless
   * options.healthCheck === false or options.healthCheck.enabled === false.
   * @returns {function} cancel function
   */
  startSystemHealthCheck() {
    if (this.systemHealthCheckIntervalId) {
      return this.cancelSystemHealthCheck;
    }

    const intervalMs = this.systemHealthCheckIntervalMinutes * 60 * 1000;
    this.logger.info(
      `Starting health check interval: every ${this.systemHealthCheckIntervalMinutes} minutes` +
      `${this.autoRestartOnStuck ? ' (with auto-restart on stuck)' : ''}`
    );

    const handleResult = (result) => {
      if (this.autoRestartOnStuck && result?.systemUnresponsive) {
        this.logger.error('RESTART: System unresponsive - exiting for process manager to restart', result);
        process.exit(0);
      }
    };

    this.performSystemHealthCheck()
      .then(handleResult)
      .catch((err) => this.logger.error('Initial health check failed', err));

    this.systemHealthCheckIntervalId = setInterval(async () => {
      try {
        const result = await this.performSystemHealthCheck();
        handleResult(result);
      } catch (err) {
        this.logger.error('Health check interval error', err);
      }
    }, intervalMs);

    this.cancelSystemHealthCheck = () => {
      if (this.systemHealthCheckIntervalId) {
        clearInterval(this.systemHealthCheckIntervalId);
        this.systemHealthCheckIntervalId = null;
        this.logger.info('Health check interval stopped');
      }
    };

    return this.cancelSystemHealthCheck;
  }

  getSystemHealthStatus() {
    return this._systemHealth;
  }

  getSystemHealthReport() {
    return {
      current: this._systemHealth,
      lastFullCheck: this._systemHealth.lastFullCheck,
    };
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
    if (typeof this.cancelSystemHealthCheck === 'function') {
      this.cancelSystemHealthCheck();
    } else if (this.systemHealthCheckIntervalId) {
      clearInterval(this.systemHealthCheckIntervalId);
      this.systemHealthCheckIntervalId = null;
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
module.exports.promClient = client
