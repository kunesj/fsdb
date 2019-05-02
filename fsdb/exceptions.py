#!/usr/bin/python
# -*- coding: utf-8 -*-


class FsdbError(Exception):
    """
    Base exception type for all exceptions in this package
    """
    pass


class FsdbObjectNotFound(FsdbError):

    def __init__(self, message=None):
        message = message if message else 'Object not found!'
        super().__init__(message)


class FsdbObjectDeleted(FsdbError):

    def __init__(self, message=None):
        message = message if message else 'Can\'t access deleted objects!'
        super().__init__(message)


class FsdbDatabaseClosed(FsdbError):

    def __init__(self, message=None):
        message = message if message else 'Database is closed!'
        super().__init__(message)


class FsdbDomainError(FsdbError):

    def __init__(self, domain, message=None):
        message = message if message else 'Invalid search domain!'
        super().__init__('{} {}'.format(message, domain))


class FsdbOrderError(FsdbError):

    def __init__(self, message=None):
        message = message if message else 'Invalid record order!'
        super().__init__(message)
