# -*- coding: utf-8 -*-
#
# This file is part of TimeGate.
# Copyright (C) 2014, 2015 LANL.
# Copyright (C) 2016 CERN.
#
# TimeGate is free software; you can redistribute it and/or modify
# it under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Implementation of the TimeGate caches."""

from __future__ import absolute_import, print_function

import logging
import os
import sys
from datetime import datetime

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from werkzeug.contrib.cache import FileSystemCache, md5
from werkzeug.utils import import_string


class Cache(object):
    """Base class for TimeGate caches."""

    def __init__(self, cache_backend, cache_refresh_time=86400,
                 max_file_size=0, **kwargs):
        """Constructor method.

        :param cache_backend: Importable string pointing to cache class.
        :param max_file_size: (Optional) The maximum size (in Bytes) for a
        TimeMap cache value. When max_file_size=0, there is no limit to
        a cache value. When max_file_size=X > 0, the cache will not
        store TimeMap that require more than X Bytes on disk.
        """
        self.tolerance = relativedelta(seconds=cache_refresh_time)
        self.max_file_size = max(max_file_size, 0)
        self.CHECK_SIZE = self.max_file_size > 0
        self.backend = import_string(cache_backend)(**kwargs)

    def get_until(self, uri_r, date):
        """Returns the TimeMap (memento,datetime)-list for the requested
        Memento. The TimeMap is guaranteed to span at least until the 'date'
        parameter, within the tolerance.

        :param uri_r: The URI-R of the resource as a string.
        :param date: The target date. It is the accept-datetime for TimeGate
        requests, and the current date. The cache will return all
        Mementos prior to this date (within cache.tolerance parameter)
        :return: [(memento_uri_string, datetime_obj),...] list if it is
        in cache and if it is within the cache tolerance for *date*,
        None otherwise.
        """
        # Query the backend for stored cache values to that memento
        val = self.backend.get(uri_r)
        if val:  # There is a value in the cache
            timestamp, timemap = val
            if date <= timestamp + self.tolerance:
                return timemap

    def get_all(self, uri_r):
        """Request the whole TimeMap for that uri.

        :param uri_r: the URI-R of the resource.
        :return: [(memento_uri_string, datetime_obj),...] list if it is in
        cache and if it is within the cache tolerance, None otherwise.
        """
        until = datetime.utcnow().replace(tzinfo=tzutc())
        return self.get_until(uri_r, until)

    def set(self, uri_r, timemap):
        """Set the cached TimeMap for that URI-R.

        It appends it with a timestamp of when it is stored.

        :param uri_r: The URI-R of the original resource.
        :param timemap: The value to cache.
        :return: The backend setter method return value.
        """
        timestamp = datetime.utcnow().replace(tzinfo=tzutc())
        val = (timestamp, timemap)
        if self._check_size(val):
            self.backend.set(uri_r, val)

    def _check_size(self, val):
        """Check the size that a specific TimeMap value is using in memory.

        It deletes if it is more than the maximum size.

        :param val: The cached object.
        :return: The True if it can be stored.
        """
        if self.CHECK_SIZE:
            size = sys.getsizeof(val)
            if size > self.max_file_size:
                return False
        return True
