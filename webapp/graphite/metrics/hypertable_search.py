import sys
import time
import subprocess
import os.path
from django.conf import settings
from graphite.logger import log
import re

from graphite.hypertable_client import HyperTablePool

EXPANDABLE_PATH_RE = re.compile('.*[\*{}\[\]]+.*')
def regexifyPathExpr(pathExpr):
  return '^%s$' % re.sub('\*', '[^\.]+', re.sub('\.', '\.', pathExpr))

# SELECT * FROM metrics WHERE (ROW = "x" OR ROW = "y") REVS 1;

class HyperStore:
  def find(self, pathExpr):
    if EXPANDABLE_PATH_RE.match(pathExpr):
      regex = regexifyPathExpr(pathExpr)
      where = 'ROW REGEXP "%s"' % regex

      starIndex = pathExpr.find('*')
      if starIndex > 0:
        where += ' AND ROW =^ "%s"' % pathExpr[0:starIndex]

      return self.findHelper(where)
    else:
      return [pathExpr]

  def findByRegex(self, regex):
    where = 'ROW REGEXP "%s"' % regex
    return self.findHelper(where)

  def findHelper(self, where):
    query = 'SELECT * FROM search WHERE %s' % (where)

    metrics = []
    def processResult(key, family, column, val, ts):
      metrics.append(key)

    HyperTablePool.doQuery(query, processResult)
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

  def find(self, query):
    query_parts = query.split('.')
  
    pattern = '.'.join(query_parts[0:-1]) + '|'
    query = 'SELECT * FROM tree WHERE row =^ "%s"' % pattern

    nodes = []
    def processResult(key, family, column, val, ts):
      if column == 'has_children':
        nodes.append(HyperNode(key.replace('|', '.'), val == '0'))

    HyperTablePool.doQuery(query, processResult)
    return nodes

hypertable_searcher = HyperTableIndexSearcher()