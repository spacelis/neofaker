#!/usr/bin/env python3
"""
File: rankdom_person.py
Author: Wen Li Email: spacelis@gmail.com
Github: http://github.com/spacelis
Description: Generate a CSV listing some ramdom persons
"""

import re
import sys
import random as rdm
from csv import DictWriter
from functools import reduce
from itertools import groupby
from faker import Factory
import click

HOUSEHOLD_SIZES = {
    1: 10,
    2: 10,
    3: 6,
    4: 2,
    5: 1,
    6: 1
}

HOUSEHOLD_SIZE_RAND = reduce(lambda x, y: x + y,
                             [[p] * s for p, s in HOUSEHOLD_SIZES.items()], [])

WHITESPACES = re.compile(r'\s+')


class RandomProperty(object):
    """ Construct a random property of persons"""

    schema = [{  # a list of NodeSpec s
        'node_type': 'Name',
        'columns': [{
            'colname': 'forename',
            'rel_type': 'FORNAME'
        }, {
            'colname': 'surname',
            'rel_type': 'SURNAME'
        }],
        'id_prefix': 'Name-'
    }, {
        'node_type': 'Address',
        'columns': [{
            'colname': 'address',
            'rel_type': 'LIVE_AT'
        }],
        'id_prefix': 'Address-'
    }]

    def __init__(self, faker=None):
        super(RandomProperty, self).__init__()
        self.faker = faker or Factory.create()
        self.surnames = [self.faker.last_name()
                         for _ in range(rdm.randrange(1, 2))]
        self.address = WHITESPACES.sub(' ', self.faker.address())
        self.person_num = rdm.choice(HOUSEHOLD_SIZE_RAND)
        self.records = [
            {
                'forename': self.faker.first_name(),
                'surname': rdm.choice(self.surnames),
                'address': self.address
            } for _ in range(self.person_num)
        ]


    @classmethod
    def random_records(cls, faker=None, num=100):
        """ A generator of persons in households

        :faker: a faker object for generating names and addresses
        :returns: a list of generated records with each row a person record

        """
        faker = faker or Factory.create()
        for _ in range(num):
            for p in cls(faker).records:
                yield p
