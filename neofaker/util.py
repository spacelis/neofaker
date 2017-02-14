"""
File: util.py
Author: Wen Li Email: spacelis@gmail.com
Github: http://github.com/spacelis
Description: A set of utility functions
"""

from functools import reduce
from csv import DictWriter

def rekey(dct, kname, vname, extra=None):
    """ Generate dicts by keying the key-value pair

    :dct: a dict
    :kname: the name for the key
    :vname: the name for the value
    :returns: a set of dicts, each item in dct is repack as a dict

    """
    for k, v in dct.items():
        yield mk_dict({kname: k, vname: v}, extra if extra else {})


def to_csv(fobj, items):
    """ Return random items in CSV

    :fobj: A file object
    :items: A generator of dict items
    :returns: None

    """
    first = next(items)
    wr = DictWriter(fobj, list(first.keys()))
    wr.writeheader()
    wr.writerow(first)
    for item in items:
        wr.writerow(item)


def mk_dict(*args):
    """Make a new dict from a series of dict

    :*args: dicts
    :returns: a combined dicts

    """
    return dict(reduce(lambda x, y: x + y, [list(d.items()) for d in args], []))


def number(lst, prefix, start=0):
    """ Number the items in the lst

    :lst: contains items to number
    :returns: a dict with item as the key and its number as the value

    """
    return {item: '{0}{1}'.format(prefix, itemId)
            for itemId, item in enumerate(sorted(lst), start=start)}
