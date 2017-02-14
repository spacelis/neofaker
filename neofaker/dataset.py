#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File: dataset.py
Author: Wen Li
Email: spacelis@gmail.com
Github: http://github.com/spacelis
Description: Data set processing
"""

from csv import DictReader
from functools import reduce
from .util import number, rekey, to_csv


class ColumnSpec(object):
    """ A specification for column"""
    def __init__(self, colname, rel_type):
        super(ColumnSpec, self).__init__()
        self.colname = colname
        self.rel_type = rel_type

    @classmethod
    def from_dict(cls, obj):
        """ load from json
        :returns: return a ColumnSpec per obj

        """
        return cls(obj['colname'], obj['rel_type'])


class NodeSpec(object):
    """ Specification of how nodes can be construct from records."""
    def __init__(self, node_type, columns,
                 valname='value', id_prefix='node', id_start=0):
        super(NodeSpec, self).__init__()
        self.node_type = node_type.upper()
        self.columns = columns
        self.valname = valname
        self.id_prefix = id_prefix
        self.id_start = id_start

    @classmethod
    def from_dict(cls, obj):
        """ Load from dict

        :obj: A dict object
        :returns: A NodeSpec per the dict object

        """
        return cls(obj['node_type'],
                   [ColumnSpec.from_dict(c) for c in obj['columns']],
                   obj.get('valname', 'value'),
                   obj.get('id_prefix', 'node'),
                   obj.get('id_start', 0)
                  )

    @classmethod
    def from_dicts(cls, objs):
        """ Make a set of node specs from objs

        :objs: A list of objs
        :returns: A list of NodeSpecs

        """
        return [cls.from_dict(o) for o in objs]


def build_index(records, node_spec, id_start=0):
    """ Build a value, id mapping from the records given the node spec

    :records: a list of records in terms of dicts
    :node_spec: A node spec
    :returns: a value to id mapping dict

    """
    vals = set([r[col.colname] for col in node_spec.columns for r in records])
    return number(vals, node_spec.id_prefix, node_spec.id_start)


def build_node(idx, node_type):
    """ Build node list

    :idx: a value to id mapping dict
    :node_type: a string describe the node type
    :returns: a list of records of the nodes extracted from the mapping

    """
    return rekey(idx, 'value', 'id:ID', {':LABEL': node_type})


def mk_rec_id(rec_type, i):
    """ Compose a record node id

    :rec_type: a string describe the type of records
    :i: an integer for the id part
    :returns: a string of node id

    """
    return '{0}-{1}'.format(rec_type, i)


def build_rec_nodes(records, rec_type):
    """ Build record nodes

    :records: a list of records in terms of dicts
    :returns: a list of dicts conforming to the Neo4J importting format

    """
    return [{'value': rec_type,
             'id:ID': mk_rec_id(rec_type, i),
             ':LABEL': rec_type} for i in range(len(records))]


def build_relationship(records, rec_type, idx, node_spec):
    """ Build relationships for each record

    :records: a list of records
    :rec_type: a string describe the label to be used in graph
    :idx: a value to id mapping for the encoding relationship
    :node_spec: the node_spec of the relationship type
    :returns: A list of records of relationship between records to attributes.

    """
    return [{
        ':START_ID': mk_rec_id(rec_type, i),
        ':TYPE': col.rel_type,
        ':END_ID': idx[r[col.colname]]
    } for col in node_spec.columns for i, r in enumerate(records)]


class DataSet(object):
    """ A data set of persons"""
    def __init__(self, records, node_specs, rec_type='Record'):
        super(DataSet, self).__init__()
        self.records = records

        indices = [build_index(self.records, ns)
                   for ns in node_specs]
        attr_nodes_list = [build_node(idx, s.node_type)
                           for idx, s in zip(indices, node_specs)]
        rec_nodes = build_rec_nodes(records, rec_type)

        relationship_list = [build_relationship(records, rec_type, idx, ns)
                             for idx, ns in zip(indices, node_specs)]

        self.nodes = rec_nodes + reduce(lambda x, y: list(x) + list(y), attr_nodes_list, [])
        self.relationships = reduce(lambda x, y: list(x) + list(y), relationship_list, [])


    def to_graph_csv(self, prefix='graph'):
        """ Write the dataset to the files in prefix

        :prefix: a filename prefix for the generated CSV files for the graph
        :returns: None

        """

        with open(prefix + '.nodes', 'w') as fnodes:
            to_csv(fnodes, iter(self.nodes))
        with open(prefix + '.relationships', 'w') as frels:
            to_csv(frels, iter(self.relationships))

    @classmethod
    def from_csv(cls, fobj, node_specs):
        """ Load records from CSV

        :filename: the CSV file to read for records
        :returns: a data set containing the records and ready to produce graphs

        """
        rd = DictReader(fobj)
        return cls(list(rd), node_specs)
