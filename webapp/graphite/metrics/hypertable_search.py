import sys
import time
import subprocess
import os.path
from django.conf import settings
from graphite.logger import log
import re

from graphite.hypertable_client import HYPERTABLE_CLIENT

EXPANDABLE_PATH_RE = re.compile('.*[\*{}\[\]]+.*')
def regexifyPathExpr(pathExpr):
  return '^%s$' % re.sub('\*', '[^\.]+', re.sub('\.', '\.', pathExpr))

# SELECT * FROM metrics WHERE (ROW = "x" OR ROW = "y") REVS 1;

class HyperStore:
  def find(self, pathExpr):
    if EXPANDABLE_PATH_RE.match(pathExpr):
      pathExpr = regexifyPathExpr(pathExpr)
      return self.findByRegex(pathExpr)
    else:
      return [pathExpr]

  def findByRegex(self, query): 
    query = 'SELECT * FROM search WHERE ROW REGEXP "%s"' % (query)

    log.info('running query: %s' % query)
    results = HYPERTABLE_CLIENT.hql_exec2(HYPERTABLE_CLIENT.namespace_open('monitor'), query, 0, 1)
    log.info('done running query: %s' % query)

    metrics = []
    while True:
      row_data = HYPERTABLE_CLIENT.next_row_as_arrays(results.scanner)
      if not row_data:
        break
      for metric_path, _, _, val, _ in row_data:
        metrics.append(metric_path)
    return metrics


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
