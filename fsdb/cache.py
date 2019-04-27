#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import logging

_logger = logging.getLogger(__name__)


class Cache(object):

    def __init__(self, cache_size=100*(1024**2)):
        self.cache_size = cache_size
        self.cache_size_limit = int(cache_size*1.5)  # when to trigger clean
        self.cache = {}
        self.access_list = []

    def to_cache(self, key, value):
        # don't cache objects larger then min cache size
        if sys.getsizeof(value) > self.cache_size:
            return
        # set cache value
        self.cache[key] = value
        # update access "time"
        if key in self.access_list:
            self.access_list.remove(key)
        self.access_list.append(key)
        # remove old items if over cache size limit
        if sys.getsizeof(self.cache) > self.cache_size_limit:
            while sys.getsizeof(self.cache) > self.cache_size and len(self.access_list) > 0:
                self.del_cache(self.access_list[0])

    def from_cache(self, key):
        if key in self.cache:
            # update access "time"
            if key in self.access_list:
                self.access_list.remove(key)
            self.access_list.append(key)
            # return cache value
            return self.cache[key]
        else:
            return None

    def del_cache(self, key):
        if key in self.cache:
            if key in self.access_list:
                self.access_list.remove(key)
            del(self.cache[key])

    def clear(self):
        self.cache = {}
        self.access_list = []
