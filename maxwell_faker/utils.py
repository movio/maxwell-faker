# -*- coding: utf-8 -*-

import sys
import zlib

CONSONANTS = [ 'b', 'c', 'd', 'f', 'g', 'j', 'k', 'm', 'n', 'p', 'r', 's', 't', 'v', 'z' ]
VOWELS = [ 'a', 'e', 'i', 'o', 'u', 'y' ]

SYLLABLES = [ c + v for c in CONSONANTS for v in VOWELS ]

def usage(msg):
    sys.stderr.write('Error: ' + msg + '\n')
    sys.exit(1)

def fake_random_int(seed, specifier, i, a, b = None):
    if b is None: a, b = 0, a
    a, b = long(a), long(b)
    return (zlib.crc32(str(seed) + str(specifier) + str(i) + str(a) + str(b)) % (b - a)) + a

def fake_random_float(seed, specifier, i, a, b = None):
    if b is None: a, b = 0, a
    return fake_random_int(seed, specifier, i, a * 100, b * 100) / 100.0

def fake_random_string(seed, specifier, i, a, b = None):
    if b is None: a, b = 0, a
    length = fake_random_int(seed, [specifier, 'length'], i, a, b)
    result = ""
    while len(result) < length:
        index = fake_random_int(seed, [specifier, 'syllable', str(len(result))], i, 0, len(SYLLABLES))
        result += SYLLABLES[index]
    return result[:length]

