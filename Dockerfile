FROM pypy:2-slim

COPY . /maxwell-faker

RUN cd /maxwell-faker && pip install .
