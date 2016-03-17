# -*- coding: utf-8 -*-

import sys
import hashlib

def usage(msg):
    sys.stderr.write('Error: ' + msg + '\n')
    sys.exit(1)

def fake_random_int(seed, nounce, i, a, b = None):
    if b is None: a, b = 0, a
    m = hashlib.md5()
    m.update(seed)
    if type(nounce) is list:
        for nounce_item in nounce: m.update(nounce_item)
    else:
        m.update(nounce)
    m.update(str(i))
    m.update(str(b))
    return (long(m.hexdigest(), 16) % (b-a)) + a

def fake_random_float(seed, nounce, i, a, b = None):
    if b is None: a, b = 0, a
    return fake_random_int(seed, nounce, i, a * 100, b * 100) / 100.0

