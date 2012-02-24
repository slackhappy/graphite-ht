from django.conf import settings
from graphite.logger import log
import hypertable.thriftclient

HYPERTABLE_CLIENT = None

if settings.HYPERTABLE_SERVER:
  log.info('have hypertable server @ %s' % settings.HYPERTABLE_SERVER)
  (host, port) = settings.HYPERTABLE_SERVER.split(':')
  HYPERTABLE_CLIENT = hypertable.thriftclient.ThriftClient(host, int(port))
  log.info('now have client!')