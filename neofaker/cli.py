#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: cli.py
Author: Wen Li
Email: spacelis@gmail.com
Github: http://github.com/spacelis
Description: commandline
"""

import sys
import json
import click
from faker import Factory
from .personprop import RandomProperty
from .dataset import DataSet, NodeSpec
from .util import to_csv

PREDEFINED_SCHEMAS = {
    'rp': RandomProperty.schema
}

@click.group()
def console():
    """ Command grouper """
    pass


def collect_node_specs(ref):
    """ Collect node specs via given ref

    :ref: TODO
    :returns: TODO

    """
    try:
        with open(ref) as fin:
            return NodeSpec.from_dicts(json.load(fin))
    except IOError:
        pass
    return PREDEFINED_SCHEMAS[ref]


@console.command()
@click.option('-o', '--output-prefix', default='graph')
@click.option('-s', '--schema')
@click.argument('csvfile', type=click.File())
def csv2graph(schema, csvfile, output_prefix):
    """ Convert CSV records to nodes and relationship CSVs of Neo4J"""
    node_specs = collect_node_specs(schema)
    dts = DataSet.from_csv(csvfile, NodeSpec.from_dicts(node_specs))
    dts.to_graph_csv(output_prefix)


@console.command()
@click.option('--num', '-n', type=int, default=100,
              help='Number of records to generate')
@click.argument('output', type=click.File('w'))
def randp(num, output):
    """ Generate random properties with random person info

    :num: Number of property to generate
    :output: the outpu file
    :returns: TODO

    """
    faker = Factory.create('en_GB')
    to_csv(output, RandomProperty.random_records(faker, num))


if __name__ == "__main__":
    console()
