FROM pypy:2

COPY . /maxwell-faker

RUN cd /maxwell-faker && pip install .
