from __future__ import with_statement

from django.conf import settings
from graphite.logger import log
import hypertable.thriftclient
import threading

class ConnectionPool:
  def makeClient(self):
    (host, port) = settings.HYPERTABLE_SERVER.split(':')
    return hypertable.thriftclient.ThriftClient(host, int(port))
  
  def __init__(self, count):
    self.freeClients = [self.makeClient() for x in range(0, count)]
    self.semaphore = threading.BoundedSemaphore(count)
    self.lock = threading.Lock()

  def getConn(self):
    with self.lock:
      return self.freeClients.pop()

  def releaseConn(self, conn):
    with self.lock:
      return self.freeClients.append(conn)

  def doQuery(self, query, cb):
    with self.semaphore:
      conn = self.getConn()
      namespace = conn.namespace_open('monitor')
      results =  conn.hql_exec2(namespace, query, 0, 1)

      while True:
        row_data = conn.next_row_as_arrays(results.scanner)
        if not row_data:
          break
        for key, family, column, val, ts in row_data:
          cb(key, family, column, val, ts)

      conn.close_scanner(results.scanner)
      self.releaseConn(conn)

HyperTablePool = ConnectionPool(20)
