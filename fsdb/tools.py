#!/usr/bin/python
# -*- coding: utf-8 -*-
from .exceptions import FsdbOrderError, FsdbDomainError

import re
import copy


def sanitize_filename(filename):
    assert isinstance(filename, str)  # bytes type not supported
    filename = filename.strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '_', filename)


def validate_order(order):
    if not isinstance(order, str):
        raise FsdbOrderError('Order must be string!')
    for o in order.split(','):
        o = o.strip()
        if o == '':
            raise FsdbOrderError
        if len(o.split(' ')) != 2:
            raise FsdbOrderError
        if o.split(' ')[-1].lower() not in ['asc', 'desc']:
            raise FsdbOrderError


def validate_domain(domain, valid_fields):
    domain = copy.deepcopy(domain if domain else [])
    if not isinstance(domain, list):
        raise FsdbDomainError(domain, 'Domain must be list!')
    if len(domain) == 0:
        return

    # check number and type of values
    for dom in domain:
        # &, |
        if isinstance(dom, str):
            if dom not in ['&', '|']:
                raise FsdbDomainError(domain)
            continue

        # filters
        if not (isinstance(dom, tuple) or isinstance(dom, list)):
            raise FsdbDomainError(domain)
        if len(dom) != 3:
            raise FsdbDomainError(domain)

        dom_field, dom_eq, dom_value = tuple(dom)
        if not isinstance(dom_field, str) or not isinstance(dom_eq, str):
            raise FsdbDomainError(domain)
        if dom_field not in valid_fields:
            raise FsdbDomainError(domain)
        if dom_eq in ['in', 'not in'] and not isinstance(dom_value, list):
            raise FsdbDomainError(domain)

    # fake process domain
    for i, dom in enumerate(domain):
        if not isinstance(dom, str):
            domain[i] = True

    # validate domain logic
    try:
        evaluate_domain(domain)
    except FsdbDomainError:
        raise
    except Exception:
        raise FsdbDomainError(domain)


def evaluate_domain(domain):
    """
    :param domain: processed domain
    :return: result (boolean)
    """
    domain = copy.deepcopy(domain)
    if len(domain) == 0:
        return True

    domain_changed = True
    while domain_changed:
        domain_changed = False

        # [bool, bool, ...] -> [(bool and bool), ...]
        if len(domain) >= 2 and isinstance(domain[0], bool) and isinstance(domain[0], bool):
            domain[0] = domain[0] and domain[1]
            domain.pop(1)
            domain_changed = True

        # bool, bool, bool -> bool, (bool and bool)
        for i in range(len(domain)):
            if len(domain) <= i+2:
                continue

            val1 = domain[i]
            val2 = domain[i+1]
            val3 = domain[i+2]

            if isinstance(val1, bool) and isinstance(val2, bool) and isinstance(val3, bool):
                domain[i+1] = val2 and val3
                domain.pop(i+2)
                domain_changed = True
                break

        # op, bool, bool -> (bool op bool)
        for i in range(len(domain)):
            if len(domain) <= i+2:
                continue

            val1 = domain[i]
            val2 = domain[i+1]
            val3 = domain[i+2]

            if isinstance(val1, str) and isinstance(val2, bool) and isinstance(val3, bool):
                if val1 == '&':
                    domain[i] = val2 and val3
                elif val1 == '|':
                    domain[i] = val2 or val3
                else:
                    raise FsdbDomainError(domain)
                domain.pop(i+2)
                domain.pop(i+1)
                domain_changed = True
                break

    if len(domain) != 1:
        raise FsdbDomainError(domain)

    if not isinstance(domain[0], bool):
        raise FsdbDomainError(domain)

    return domain[0]





