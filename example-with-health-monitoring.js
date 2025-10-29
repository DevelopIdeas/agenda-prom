const path = require('path')
require('dotenv').config({ path: path.join(__dirname, './', '.env') })

const _ = require('lodash');
const Agenda = require('agenda');
const AgendaMetricsCollector = require('agenda-prom');

const moment = require('moment')

global.__app_root = __dirname;

process.env.LOG_FILENAME = 'agenda-motormoolah.log';
const { createLogger } = require('winston-custom');
const logger = createLogger({ consoleLevel: 'debug', fileLevel: 'debug' });
global.__logger = logger;

const { dbWarehouse, dbMax, find, updateWarehouse, QueryTypes } = require('./utils/db');

const agendaConnectionStr = `mongodb://127.0.0.1:27017/agenda-tcg-aftersale`;

__logger.info('Agenda Release', { release: process.env.RELEASE || 'UNKNOWN' })

const AgendaConntionManager = require('agenda-connection-manager');
const { syncCustomerObjects, syncWarehouseMaxHistory, syncPredictiveAi, submitPredictiveAi, syncTcInvoices } = require('./utils/sync-db');
const { importFromSFTP } = require('./utils/leads');
const { submitUpdates } = require('./utils/tcg-api');
const { fetchDvlaMotDetails, fetchDvlaDetails } = require('./utils/dvla');
const aConMan = new AgendaConntionManager()

const agenda = new Agenda({
  db: {
    address: agendaConnectionStr, collection: 'agenda', useUnifiedTopology: true
  },
  maxConcurrency: 5,
  defaultConcurrency: 10
}, (err, collection) => {
  if (err) {
    __logger.error('Agenda Connection Error', err)
    aConMan.setCollection(collection)
    aConMan.setConnected(false, err)
  } else {
    __logger.info('Agenda Connected')
    aConMan.setCollection(collection)
    aConMan.setConnected(true)
  }
});

// Initialize metrics collector with health monitoring
const metricsCollector = new AgendaMetricsCollector(agenda, {
  processName: process.env.name || 'UNKNOWN',
  alloyMode: 'file',
  metricsFilePath: `/home/deploy/agenda-metics/agenda_metrics_${process.env.name}.prom`,
  updateInterval: 10000, // Update metrics every 10 seconds
  
  // Health monitoring configuration
  sequelize: dbWarehouse, // Use your existing dbWarehouse Sequelize instance
  healthMetricsDirectory: path.join(__dirname, 'health-checks'), // Directory for health check JSON files
  healthCheckInterval: 30000 // Check health every 30 seconds
});

const syncResultsJob = `sync-results-${process.env.RELEASE}`;
metricsCollector.defineJob(syncResultsJob, { concurrency: 1 }, async (job, done) => {
  const now = moment()
  try {
    await syncCustomerObjects()
  } catch (ex) {
    __logger.error('syncCustomerObjects Error', ex)
  }
  try {
    await syncWarehouseMaxHistory()
  } catch (ex) {
    __logger.error('syncWarehouseMaxHistory Error', ex)
  }
  try {
    await importFromSFTP()
  } catch (ex) {
    __logger.error('importFromSFTP Error', ex)
  }
  try {
    await submitPredictiveAi(false)
  } catch (ex) {
    __logger.error('submitPredictiveAi Error', ex)
  }
  try {
    await syncPredictiveAi()
  } catch (ex) {
    __logger.error('syncPredictiveAi Error', ex)
  }
  try {
    await syncPredictiveAi('ttcg-aftersales-status')
  } catch (ex) {
    __logger.error('syncPredictiveAi - immediate Error', ex)
  }
  try {
    await submitUpdates()
  } catch (ex) {
    __logger.error('submitUpdates Error', ex)
  }
  try {
    const rowsDvlaCheck = await find(`
      select 
        unique_id,
        vehicle_reg
      from customer c
      where (dvla_checked is null || dvla_checked = 0) and vehicle_reg is not null and vehicle_reg <> ''
    `)

    let rowIndx=0
    for (let r of rowsDvlaCheck) {
      await fetchDvlaDetails(r.vehicle_reg, r, rowIndx, rowsDvlaCheck.length)
      await fetchDvlaMotDetails(r.vehicle_reg, r, rowIndx, rowsDvlaCheck.length)

      await updateWarehouse('', [{ unique_id: r.unique_id, dvla_checked: 1 }], 'customer', ['unique_id'])
      rowIndx++
    }

    await dbWarehouse.query(`
      UPDATE customer c
      LEFT JOIN dvla_vrm_lookup dvl ON dvl.vrm = REPLACE(c.vehicle_reg, ' ', '')
      SET c.vehicle_age = TIMESTAMPDIFF(MONTH, dvl.month_of_first_registration, c.created_at)
      WHERE dvl.vrm IS NOT NULL 
        AND dvl.month_of_first_registration IS NOT NULL 
        AND c.vehicle_age IS NULL;
    `, {
      type: QueryTypes.UPDATE,
      replacements: {}
    })
  } catch (ex) {
    __logger.error('DVLA Update Error', ex)
  }
  try {
    await syncTcInvoices()
  } catch (ex) {
    __logger.error('syncTcInvoices Error', ex)
  }

  done();
});

(async function() {
  try {
    __logger.debug('Db Connected')
    await dbWarehouse.authenticate();
    __logger.debug('Db Warehouse Connected')
    await dbMax.authenticate();
    __logger.debug('Db Max Connected')

    aConMan.setAgenda(agenda)
    aConMan.addJobListeners()
    await aConMan.unlockJobs()

    await agenda.start();

    const repeatOptions = { timezone: 'Europe/London' }
    await agenda.every('*/5 * * * *', syncResultsJob, {}, repeatOptions);

    __logger.info('Agenda Started')
  } catch (ex) {
    __logger.error('DB Connection Error', ex)
  }
})();