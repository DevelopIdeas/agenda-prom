// Example usage of enhanced AgendaMetricsCollector for debugging
const Agenda = require('agenda');
const AgendaMetricsCollector = require('./src/index');

// Initialize Agenda
const agenda = new Agenda({
  db: { address: 'mongodb://localhost:27017/agenda-debug' }
});

// Initialize metrics collector
const metricsCollector = new AgendaMetricsCollector(agenda, {
  alloyMode: 'file',
  processName: 'debug-test',
  updateInterval: 5000
});

// Define some test jobs
metricsCollector.defineJob('quick-job', async (job) => {
  console.log('Quick job running...');
  await new Promise(resolve => setTimeout(resolve, 1000));
  console.log('Quick job completed');
});

metricsCollector.defineJob('slow-job', async (job) => {
  console.log('Slow job running...');
  await new Promise(resolve => setTimeout(resolve, 30000)); // 30 seconds
  console.log('Slow job completed');
});

metricsCollector.defineJob('hanging-job', async (job) => {
  console.log('Hanging job running...');
  // This job will hang indefinitely to simulate stuck jobs
  await new Promise(() => {}); // Never resolves
});

metricsCollector.defineJob('failing-job', async (job) => {
  console.log('Failing job running...');
  await new Promise(resolve => setTimeout(resolve, 2000));
  throw new Error('This job always fails');
});

// Start agenda
agenda.start();

// Schedule some test jobs
agenda.ready(() => {
  console.log('Agenda ready, scheduling test jobs...');
  
  // Schedule recurring jobs
  agenda.every('30 seconds', 'quick-job');
  agenda.every('2 minutes', 'slow-job');
  
  // Schedule a hanging job
  agenda.now('hanging-job');
  
  // Schedule some failing jobs
  agenda.every('1 minute', 'failing-job');
});

// Diagnostic endpoint - call this to see what's running
function printDiagnostics() {
  const diagnostics = metricsCollector.getDiagnostics();
  console.log('\n=== DIAGNOSTICS ===');
  console.log(`Process: ${diagnostics.processName}`);
  console.log(`Connected: ${diagnostics.isConnected}`);
  console.log(`Total running jobs: ${diagnostics.totalRunningJobs}`);
  console.log(`Long running jobs:`, diagnostics.longRunningJobsCount);
  
  if (diagnostics.runningJobs.length > 0) {
    console.log('\nCurrently running jobs:');
    diagnostics.runningJobs.forEach(job => {
      console.log(`  - ${job.jobName} (${job.jobId}) - running ${job.runningTimeMinutes} minutes`);
    });
  }
  console.log('==================\n');
}

// Print diagnostics every 30 seconds
setInterval(printDiagnostics, 30000);

// Print initial diagnostics after 5 seconds
setTimeout(printDiagnostics, 5000);

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down...');
  await agenda.stop();
  metricsCollector.cleanup();
  process.exit(0);
});

console.log('Debug example started. Press Ctrl+C to stop.');
console.log('Watch the console for diagnostics and check agenda_metrics.prom for metrics.');
