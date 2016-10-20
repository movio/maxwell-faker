# -*- coding: utf-8 -*-

import io
import socket
import struct
import time

MSG_SIZE_FIELD_SIZE = 4
API_KEY_FIELD_SIZE = 2
API_VERSION_FIELD_SIZE = 2
FLAGS_FIELD_SIZE = 2
PARTITION_KEY_FIELD_SIZE = 4
TOPIC_SIZE_FIELD_SIZE = 2
TIMESTAMP_FIELD_SIZE = 8
KEY_SIZE_FIELD_SIZE = 4
VALUE_SIZE_FIELD_SIZE = 4

ANY_PARTITION_FIXED_BYTES = MSG_SIZE_FIELD_SIZE + API_KEY_FIELD_SIZE + \
                            API_VERSION_FIELD_SIZE + FLAGS_FIELD_SIZE + \
                            TOPIC_SIZE_FIELD_SIZE + TIMESTAMP_FIELD_SIZE + \
                            KEY_SIZE_FIELD_SIZE + VALUE_SIZE_FIELD_SIZE

PARTITION_KEY_FIXED_BYTES = ANY_PARTITION_FIXED_BYTES + \
                            PARTITION_KEY_FIELD_SIZE

ANY_PARTITION_API_KEY = 256
ANY_PARTITION_API_VERSION = 0

PARTITION_KEY_API_KEY = 257
PARTITION_KEY_API_VERSION = 0


def create_msg(partition, topic, key_bytes, value_bytes):
    topic_bytes = bytes(topic)

    msg_size = PARTITION_KEY_FIXED_BYTES + \
               len(topic_bytes) + len(key_bytes) + len(value_bytes)

    buf = io.BytesIO()
    flags = 0
    buf.write(struct.pack('>ihhhih', msg_size,
                          PARTITION_KEY_API_KEY,
                          PARTITION_KEY_API_VERSION, flags,
                          partition, len(topic_bytes)))
    buf.write(topic_bytes)
    buf.write(struct.pack('>qi', int(time.time() * 1000), len(key_bytes)))
    buf.write(key_bytes)
    buf.write(struct.pack('>i', len(value_bytes)))
    buf.write(value_bytes)
    result_bytes = buf.getvalue()
    buf.close()
    return result_bytes


def open_bruce_socket():
    return socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
