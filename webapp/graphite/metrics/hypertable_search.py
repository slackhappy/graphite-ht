import sys
sys.path.append('/opt/hypertable/0.9.5.5/lib/py')
sys.path.append('/opt/hypertable/0.9.5.5/lib/py/gen-py')

import time
import subprocess
import os.path
from django.conf import settings
from graphite.logger import log

from graphite.hypertable_client import HYPERTABLE_CLIENT

class HyperNode:
  context = {}

  def __init__(self, metric_path, isLeaf):
    self.metric_path = metric_path
    self.real_metric = metric_path
    self.name = metric_path.split('.')[-1]
    self.__isLeaf = isLeaf

  def isLeaf(self):
    return self.__isLeaf                                                                                                                                                                                          

class HyperTableIndexSearcher:
  def search(self, query, max_results=None, keep_query_pattern=False):
    query_parts = query.split('.')
    metrics_found = set()
    log.info("query_parts: %s" % query_parts)

  def find(self, query):
    query_parts = query.split('.')
  
    pattern = '.'.join(query_parts[0:-1]) + '|'
    query = 'SELECT * FROM tree WHERE row =^ "%s"' % pattern

    log.info('running query: %s' % query)
    results =  HYPERTABLE_CLIENT.hql_exec2(HYPERTABLE_CLIENT.namespace_open('monitor'), query, 0, 1)
    log.info('done running query: %s' % query)

    nodes = []
    while True:
      row_data = HYPERTABLE_CLIENT.next_row_as_arrays(results.scanner)
      if not row_data:
        break
      for key, family, column, val, ts in row_data:
        if column == 'has_children':
          nodes.append(HyperNode(key.replace('|', '.'), val == '0'))

    HYPERTABLE_CLIENT.close_scanner(results.scanner)
    return nodes

hypertable_searcher = None
if HYPERTABLE_CLIENT:
  hypertable_searcher = HyperTableIndexSearcher()
