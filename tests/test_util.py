#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neofaker.util import *

def test_rekey():
    """ test rekey function"""
    assert list(rekey({'a': 'b'}, 'key', 'val')) == [{'key': 'a', 'val': 'b'}]
    assert {'key': 'a', 'val': 'b'} in list(rekey({'a': 'b', 'c': 'd'}, 'key', 'val'))
