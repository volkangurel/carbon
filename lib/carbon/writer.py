"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


import time
import errno

from db import get_db
from carbon import state
from carbon.cache import MetricCache
from carbon.storage import loadStorageSchemas,loadAggregationSchemas
from carbon.conf import settings
from carbon import log, events, instrumentation

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.application.service import Service


lastCreateInterval = 0
createCount = 0
schemas = loadStorageSchemas()
agg_schemas = loadAggregationSchemas()
CACHE_SIZE_LOW_WATERMARK = settings.MAX_CACHE_SIZE * 0.95


def optimalWriteOrder(app_db):
  """Generates metrics with the most cached values first and applies a soft
  rate limit on new metrics"""
  global lastCreateInterval
  global createCount
  metrics = MetricCache.counts()

  if settings.ENABLE_BATCHED_WRITES:
    existing_metrics = app_db.batch_exists([m[0] for m in metrics])
  else:
    t = time.time()
    metrics.sort(key=lambda item: item[1], reverse=True)  # by queue size, descending
    log.debug("Sorted %d cache queues in %.6f seconds" % (len(metrics),
                                                          time.time() - t))
    existing_metrics = None

  for metric, queueSize in metrics:
    if state.cacheTooFull and MetricCache.size < CACHE_SIZE_LOW_WATERMARK:
      events.cacheSpaceAvailable()

    dbFileExists = app_db.exists(metric) if existing_metrics is None else metric in existing_metrics

    if not dbFileExists:
      createCount += 1
      now = time.time()

      if now - lastCreateInterval >= 60:
        lastCreateInterval = now
        createCount = 1

      elif createCount >= settings.MAX_CREATES_PER_MINUTE:
        # dropping queued up datapoints for new metrics prevents filling up the entire cache
        # when a bunch of new metrics are received.
        try:
          MetricCache.pop(metric)
        except KeyError:
          pass

        continue

    try:  # metrics can momentarily disappear from the MetricCache due to the implementation of MetricCache.store()
      datapoints = MetricCache.pop(metric)
    except KeyError:
      log.msg("MetricCache contention, skipping %s update for now" % metric)
      continue  # we simply move on to the next metric when this race condition occurs

    yield (metric, datapoints, dbFileExists)


def writeCachedDataPoints(app_db, seen_metrics):
  "Write datapoints until the MetricCache is completely empty"
  updates = 0
  lastSecond = 0
  while MetricCache:
    dataWritten = False

    metric_datapoints = {}
    for (metric, datapoints, dbFileExists) in optimalWriteOrder(app_db):
      dataWritten = True
      if metric not in seen_metrics:
        log.metrics(metric)
        seen_metrics.add(metric)

      if not dbFileExists:
        archiveConfig = None
        xFilesFactor, aggregationMethod = None, None

        for schema in schemas:
          if schema.matches(metric):
            log.creates('new metric %s matched schema %s' % (metric, schema.name))
            archiveConfig = [archive.getTuple() for archive in schema.archives]
            break

        for schema in agg_schemas:
          if schema.matches(metric):
            log.creates('new metric %s matched aggregation schema %s' % (metric, schema.name))
            xFilesFactor, aggregationMethod = schema.archives
            break

        if not archiveConfig:
          raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)
        log.creates("creating database metric %s (metric=%s xff=%s agg=%s)" %
                    (metric, archiveConfig, xFilesFactor, aggregationMethod))

        try:
          app_db.create(metric, archiveConfig, xFilesFactor, aggregationMethod, settings.WHISPER_SPARSE_CREATE, settings.WHISPER_FALLOCATE_CREATE)
        except OSError as e:
          if e.errno != errno.EEXIST:
            log.err("%s" % e)
        instrumentation.increment('creates')

      if settings.ENABLE_BATCHED_WRITES:
        metric_datapoints[metric] = datapoints
      else:
        try:
          t1 = time.time()
          app_db.update_many(metric, datapoints)
          t2 = time.time()
          updateTime = t2 - t1
        except:
          log.msg("Error writing to %s" % (metric))
          log.err()
          instrumentation.increment('errors')
        else:
          pointCount = len(datapoints)
          instrumentation.increment('committedPoints', pointCount)
          instrumentation.append('updateTimes', updateTime)

          if settings.LOG_UPDATES:
            log.updates("wrote %d datapoints for %s in %.5f seconds" % (pointCount, metric, updateTime))

          # Rate limit update operations
          thisSecond = int(t2)

          if thisSecond != lastSecond:
            lastSecond = thisSecond
            updates = 0
          else:
            updates += 1
            if updates >= settings.MAX_UPDATES_PER_SECOND:
              time.sleep(int(t2 + 1) - t2)

    if metric_datapoints:
      batch_size = len(metric_datapoints)
      try:
        t1 = time.time()
        batch_stats = app_db.batch_update_many(metric_datapoints)
        t2 = time.time()
        updateTime = t2 - t1
      except:
        log.msg("Error batch writing %d metrics" % batch_size)
        log.err()
        instrumentation.increment('errors')
      else:
        pointCount = sum(len(datapoints) for datapoints in metric_datapoints.itervalues())
        instrumentation.increment('committedPoints', pointCount)
        instrumentation.append('updateTimes', updateTime)
        instrumentation.append('batchSizes', batch_size)

        if settings.LOG_BATCH_UPDATES:
          log.updates("wrote %d datapoints for %d metrics in %.5f seconds" % (pointCount, batch_size, updateTime))
          if batch_stats:
            log.updates(batch_stats)

        # Rate limit update operations
        thisSecond = int(t2)

        if thisSecond != lastSecond:
          lastSecond = thisSecond
          updates = 0
        else:
          updates += batch_size
          if updates >= settings.MAX_UPDATES_PER_SECOND:
            time.sleep(int(t2 + 1) - t2)

    # Avoid churning CPU when only new metrics are in the cache
    if not dataWritten:
      time.sleep(0.1)


def writeForever():
  app_db = get_db()
  seen_metrics = set()
  while reactor.running:
    try:
      writeCachedDataPoints(app_db, seen_metrics)
    except:
      log.err()

    time.sleep(1)  # The writer thread only sleeps when the cache is empty or an error occurs


def reloadStorageSchemas():
  global schemas
  try:
    schemas = loadStorageSchemas()
  except:
    log.msg("Failed to reload storage schemas")
    log.err()


def reloadAggregationSchemas():
  global agg_schemas
  try:
    agg_schemas = loadAggregationSchemas()
  except:
    log.msg("Failed to reload aggregation schemas")
    log.err()


def shutdownModifyUpdateSpeed():
    try:
        settings.MAX_UPDATES_PER_SECOND = settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN
        log.msg("Carbon shutting down.  Changed the update rate to: " + str(settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN))
    except KeyError:
        log.msg("Carbon shutting down.  Update rate not changed")


class WriterService(Service):

    def __init__(self):
        self.storage_reload_task = LoopingCall(reloadStorageSchemas)
        self.aggregation_reload_task = LoopingCall(reloadAggregationSchemas)

    def startService(self):
        self.storage_reload_task.start(60, False)
        self.aggregation_reload_task.start(60, False)
        reactor.addSystemEventTrigger('before', 'shutdown', shutdownModifyUpdateSpeed)
        reactor.callInThread(writeForever)
        Service.startService(self)

    def stopService(self):
        self.storage_reload_task.stop()
        self.aggregation_reload_task.stop()
        Service.stopService(self)
