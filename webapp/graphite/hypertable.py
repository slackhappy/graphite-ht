from django.conf import settings

# TODO(josh): ah yes, 100% hack
import sys
sys.path.append('/opt/hypertable/0.9.5.5/lib/py')

import hypertable.thriftclient

HYPERTABLE_CLIENT = None
if settings.HYPERTABLE_SERVER:
  (host, port) = settings.HYPERTABLE_SERVER.split(':')
  HYPERTABLE_CLIENT = hypertable.thriftclient.ThriftClient(host, int(port))