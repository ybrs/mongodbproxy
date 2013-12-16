mongodbproxy
============

This is a simple proxy for mongodb.

You can use this to debug queries from your application to mongodb without
changing your application code. It parses the mongo/bson protocol on the wire,
so you can hack and slow some queries.

Although it uses gevent and can handle a couple of thousand connections with ease,
its intended to be used for development purposes.

Install
-------------
$ virtualenv env
$ ./env/bin/python setup.py develop

Running
-------------
$ ./env/bin/mongo-proxy --mongo=localhost:27017 --listen=localhost:29017

then you can connect your application to localhost:29017 and see your queries and responses.
