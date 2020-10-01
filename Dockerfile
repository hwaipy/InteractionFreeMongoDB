FROM python:3.8.5-slim-buster

RUN mkdir /ifmdb
COPY . /ifmdb
COPY ./.dockerConfig/start.py /ifmdb
WORKDIR /ifmdb
RUN python3.8 setup.py install
COPY ./.dockerConfig/defaultconfig.ini /ifmdb
RUN mv /ifmdb/defaultconfig.ini /ifmdb/config.ini

CMD ["python3.8", "start.py"]