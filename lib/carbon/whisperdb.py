from os.path import sep, dirname, join, exists
from os import makedirs

import whisper

from carbon import log
from carbon.conf import settings


# default implementation
class WhisperDB(object):
    __slots__ = ('dataDir',)

    def __init__(self, dataDir):
        self.dataDir = dataDir

        if settings.WHISPER_AUTOFLUSH:
            log.msg("Enabling Whisper autoflush")
            whisper.AUTOFLUSH = True

        if settings.WHISPER_FALLOCATE_CREATE:
            if whisper.CAN_FALLOCATE:
                log.msg("Enabling Whisper fallocate support")
            else:
                log.err("WHISPER_FALLOCATE_CREATE is enabled but linking failed.")

        if settings.WHISPER_LOCK_WRITES:
            if whisper.CAN_LOCK:
                log.msg("Enabling Whisper file locking")
                whisper.LOCK = True
            else:
                log.err("WHISPER_LOCK_WRITES is enabled but import of fcntl module failed.")


    # private method
    def getFilesystemPath(self, metric):
        metric_path = metric.replace('.', sep).lstrip(sep) + '.wsp'
        return join(self.dataDir, metric_path)

    # public API
    def info(self, metric):
        return whisper.info(self.getFilesystemPath(metric))

    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        return whisper.setAggregationMethod(self.getFilesystemPath(metric), aggregationMethod, xFilesFactor)

    def create(self, metric, archiveConfig, xFilesFactor=None, aggregationMethod=None, sparse=False, useFallocate=False):
        dbFilePath = self.getFilesystemPath(metric)
        dbDir = dirname(dbFilePath)

        try:
            if not (exists(dbDir)):
                makedirs(dbDir, 0x755)
        except Exception as e:
            print("Error creating dir " + dbDir)
            raise e
        return whisper.create(dbFilePath, archiveConfig, xFilesFactor, aggregationMethod, sparse, useFallocate)

    def update_many(self, metric, datapoints):
        return whisper.update_many(self.getFilesystemPath(metric), datapoints)

    def exists(self, metric):
        return exists(self.getFilesystemPath(metric))

def NewWhisperDB():
    return WhisperDB(settings.LOCAL_DATA_DIR)
