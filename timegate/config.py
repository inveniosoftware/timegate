# -*- coding: utf-8 -*-
#
# This file is part of TimeGate.
# Copyright (C) 2016 CERN.
#
# TimeGate is free software; you can redistribute it and/or modify
# it under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Implement default configuration and custom loaders."""

from __future__ import absolute_import, print_function

import re

from configparser import ConfigParser

from ._compat import string_types


class Config(dict):
    """Implement custom loaders to populate dict."""

    def __init__(self, root_path, defaults=None):
        """Build an empty config wrapper.

        :param root_path: Path to which files are read relative from.
        :param defaults: An optional dictionary of default values.
        """
        dict.__init__(self, defaults or {})
        self.root_path = root_path

    def from_inifile(self, filename, silent=True):
        """Update the values in the config from an INI file."""
        conf = ConfigParser()
        with open(filename) as f:
            conf.read_file(f)

        # Server configuration
        self['HOST'] = conf.get('server', 'host').rstrip('/')
        self['STRICT_TIME'] = conf.getboolean('server', 'strict_datetime')
        if conf.has_option('server', 'api_time_out'):
            self['API_TIME_OUT'] = conf.getfloat('server', 'api_time_out')

        # Handler configuration
        def build_handler(section):
            """Build handler configuration."""
            output = {}
            if conf.has_option(section, 'handler_class'):
                output['HANDLER_MODULE'] = conf.get(section, 'handler_class')
            if conf.has_option(section, 'base_uri'):
                output['BASE_URI'] = conf.get(section, 'base_uri')
            if conf.getboolean(section, 'is_vcs'):
                output['RESOURCE_TYPE'] = 'vcs'
            else:
                output['RESOURCE_TYPE'] = 'snapshot'

            if conf.has_option(section, 'use_timemap'):
                output['USE_TIMEMAPS'] = conf.getboolean(section,
                                                         'use_timemap')
            else:
                output['USE_TIMEMAPS'] = False
            return output

        self.setdefault('HANDLERS', {})
        re_handler = re.compile('^handler(:(?P<handler_name>.+))?')
        for section_name in conf.sections():
            handler = re_handler.match(section_name)
            if handler:
                handler_name = handler.groupdict()['handler_name']
                section = build_handler(section_name)
                if handler_name or handler.groups()[0]:
                    self['HANDLERS'][handler_name] = section
                else:
                    self.update(section)

        # Cache
        self['CACHE_BACKEND'] = conf.get('cache', 'cache_backend')
        # Time window in which the cache value is considered young
        # enough to be valid
        self['CACHE_REFRESH_TIME'] = conf.getint('cache', 'cache_refresh_time')

        options = {
            'cache_backend': None,
            'cache_refresh_time': None,
            'default_timeout': 'getint',
            'mode': 'getint',
            'port': 'getint',
            'threshold': 'getint',
        }
        self.setdefault('CACHE_OPTIONS', {})

        for key in conf.options('cache'):
            if key in options:
                getter = options[key]
                if getter:
                    self['CACHE_OPTIONS'][key] = getattr(conf, getter)(
                        'cache', key
                    )
            else:
                self['CACHE_OPTIONS'][key] = conf.get('cache', key)

    def from_object(self, obj):
        """Update config with values from given object.

        :param obj: An import name or object.
        """
        if isinstance(obj, string_types):
            obj = import_string(obj)
        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)
