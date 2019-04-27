#!/usr/bin/python
# -*- coding: utf-8 -*-

import re


def sanitize_filename(filename):
    assert isinstance(filename, str)  # bytes type not supported
    filename = filename.strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '_', filename)
