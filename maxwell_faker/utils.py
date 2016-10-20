# -*- coding: utf-8 -*-

import sys
import zlib

CONSONANTS = [ 'b', 'c', 'd', 'f', 'g', 'j', 'k', 'm', 'n', 'p', 'r', 's', 't', 'v', 'z' ]
VOWELS = [ 'a', 'e', 'i', 'o', 'u', 'y' ]

SYLLABLES = [ c + v for c in CONSONANTS for v in VOWELS ]


def usage(msg):
    sys.stderr.write('Error: ' + msg + '\n')
    sys.exit(1)


def java_string_hashcode(s):
    """source: http://garage.pimentech.net/libcommonPython_src_python_libcommon_javastringhashcode/"""
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


def pseudorandom_long(specifier, lower, upper = None):
    if upper is None: lower, upper = 0, lower
    lower, upper = long(lower), long(upper)
    return (zlib.crc32("%s|%d|%d" % (specifier, lower, upper)) % (upper - lower)) + lower


def pseudorandom_float(specifier, lower, upper = None):
    if upper is None: lower, upper = 0, lower
    return pseudorandom_long(specifier, lower * 100, upper * 100) / 100.0


def pseudorandom_string(specifier, lower, upper = None):
    if upper is None: lower, upper = 0, lower
    length = pseudorandom_long([specifier, 'length'], lower, upper)
    result = ""
    while len(result) < length:
        index = pseudorandom_long([specifier, 'syllable', len(result)], 0, len(SYLLABLES))
        result += SYLLABLES[index]
    return result[:length]
