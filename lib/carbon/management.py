import traceback
from carbon import log
from carbon.db import get_db


def getMetadata(metric, key):
  if key != 'aggregationMethod':
    return dict(error="Unsupported metadata key \"%s\"" % key)

  try:
    value = get_db().info(metric)['aggregationMethod']
    return dict(value=value)
  except:
    log.err()
    return dict(error=traceback.format_exc())


def setMetadata(metric, key, value):
  if key != 'aggregationMethod':
    return dict(error="Unsupported metadata key \"%s\"" % key)
  try:
    old_value = get_db().setAggregationMethod(metric, value)
    return dict(old_value=old_value, new_value=value)
  except:
    log.err()
    return dict(error=traceback.format_exc())
