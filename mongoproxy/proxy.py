# coding=utf-8

import sys
import signal
import gevent
import struct
import logging
import bson
import argparse

from gevent.server import StreamServer
from gevent.socket import create_connection, gethostbyname

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(message)s')

OP_REPLY    = 1        # Reply to a client request. responseTo is set
OP_MSG      = 1000     # generic msg command followed by a string
OP_UPDATE   = 2001     # update document
OP_INSERT   = 2002     # insert new document
RESERVED    = 2003     # formerly used for OP_GET_BY_OID
OP_QUERY    = 2004     # query a collection
OP_GET_MORE = 2005     # Get more data from a query. See Cursors
OP_DELETE   = 2006     # Delete documents
OP_KILL_CURSORS = 2007 # Tell database client is done with a cursor

opnames = {
    OP_REPLY: ("OP_REPLY", "Reply to a client request. responseTo is set"),
    OP_MSG: ("OP_MSG", "generic msg command followed by a string"),
    OP_UPDATE: ("OP_UPDATE", "update document"),
    OP_INSERT: ("OP_INSERT", "insert new document"),
    RESERVED: ("RESERVED", "formerly used for OP_GET_BY_OID"),
    OP_QUERY: ("OP_QUERY", "query a collection"),
    OP_GET_MORE: ("OP_GET_MORE", "Get more data from a query. See Cursors"),
    OP_DELETE: ("OP_DELETE", "Delete documents"),
    OP_KILL_CURSORS: ("OP_DELETE", "Tell database client is done with a cursor")
}


def check_bit(i, offset):
   mask = 1 << offset
   return i & mask

class Operation(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def __str__(self):
        c = []
        for k, v in self.__dict__.iteritems():
            c.append("%s: %s" % (k, v))
        return "<Operation %s %s >" % (opnames[self.operation][0], ", ".join(c))

def parse_update(data, length):
    #struct OP_UPDATE {
    #    MsgHeader header;             // standard message header
    #    int32     ZERO;               // 0 - reserved for future use
    #    cstring   fullCollectionName; // "dbname.collectionname"
    #    int32     flags;              // bit vector. see below
    #    document  selector;           // the query to select the document
    #    document  update;             // specification of the update to perform
    #}
    val, pos = bson._get_int(data, 16)
    collection, pos = bson._get_c_string(data, pos)
    flags, pos = bson._get_int(data, pos)
    selector, update = "", ""
    try:
        o = bson.decode_all(data[pos:length])
        selector, update = o
    except Exception as e:
        logger.exception()

    # flags.
    # 0	Upsert	If set, the database will insert the supplied object into the collection if no matching document is found.
    # 1	MultiUpdate	If set, the database will update all matching objects in the collection. Otherwise only updates first matching doc.
    # 2-31	Reserved	Must be set to 0.
    upsert = check_bit(flags, 0)
    multi_update = check_bit(flags, 1)

    return Operation(operation=OP_UPDATE,
                     upsert=upsert,
                     multi_update=multi_update,
                     collection=collection,
                     selector=selector,
                     update=update)

def parse_query(data, length):
    # struct OP_QUERY {
    #     MsgHeader header;                 // standard message header
    #     int32     flags;                  // bit vector of query options.  See below for details.
    #     cstring   fullCollectionName ;    // "dbname.collectionname"
    #     int32     numberToSkip;           // number of documents to skip
    #     int32     numberToReturn;         // number of documents to return
    #                                       //  in the first OP_REPLY batch
    #     document  query;                  // query object.  See below for details.
    #   [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields
    #                                       //  to return.  See below for details.
    # }
    # flags:
    # 0   Reserved    Must be set to 0.
    # 1   TailableCursor  Tailable means cursor is not closed when the last data is retrieved.
    #      Rather, the cursor marks the final object's position. You can resume using the cursor later, from where it was located,
    #     if more data were received. Like any "latent cursor",
    #      the cursor may become invalid at some point (CursorNotFound) – for example if the final object it references were deleted.
    # 2   SlaveOk Allow query of replica slave. Normally these return an error except for namespace "local".
    # 3   OplogReplay Internal replication use only - driver should not set
    # 4   NoCursorTimeout The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
    # 5   AwaitData   Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.
    # 6   Exhaust Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
    # 7   Partial Get partial results from a mongos if some shards are down (instead of throwing an error)
    # 8-31    Reserved    Must be set to 0.
    flags, pos = bson._get_int(data, 16)
    tailable_cursor = check_bit(flags, 1)
    slave_ok = check_bit(flags, 2)
    oplog_replay = check_bit(flags, 3)
    no_cursor_timeout = check_bit(flags, 4)
    await_data = check_bit(flags, 5)
    exhaust = check_bit(flags, 6)
    partial = check_bit(flags, 7)

    collection, pos = bson._get_c_string(data, pos)
    number_to_skip, pos = bson._get_int(data, pos)
    number_to_return, pos = bson._get_int(data, pos)
    fields_to_return = None
    query = ""
    try:
        o = bson.decode_all(data[pos:length])
        query = o[0]
    except bson.InvalidBSON as e:
        o = []
        logger.exception("exception on bson decode")

    if len(o) == 2:
        fields_to_return = o[1]
    return Operation(operation=OP_QUERY,fields_to_return=fields_to_return,
                        tailable_cursor = tailable_cursor,
                        slave_ok = slave_ok,
                        oplog_replay = oplog_replay,
                        no_cursor_timeout = no_cursor_timeout,
                        number_to_skip=number_to_skip,
                        number_to_return=number_to_return,
                        await_data = await_data,
                        exhaust = exhaust,
                        partial = partial,
                        query=query)

def parse_delete(data, length):
    # struct {
    #     MsgHeader header;             // standard message header
    #     int32     ZERO;               // 0 - reserved for future use
    #     cstring   fullCollectionName; // "dbname.collectionname"
    #     int32     flags;              // bit vector - see below for details.
    #     document  selector;           // query object.  See below for details.
    # }
    # flags:
    # 0	SingleRemove	If set, the database will remove only the first matching document in the collection. Otherwise all matching documents will be removed.
    # 1-31	Reserved	Must be set to 0.
    zero, pos = bson._get_int(data, 16)
    collection, pos = bson._get_c_string(data, pos)
    flags, pos = bson._get_int(data, pos)
    single_remove = check_bit(flags, 0)
    try:
        o = bson.decode_all(data[pos:length])
        selector = o[0]
    except Exception as e:
        selector = ""
        logger.exception("exception on bson decode")
    return Operation(operation=OP_DELETE,
                     collection=collection,
                     single_remove=single_remove,
                     selector=selector)

def parse_reply(data, length):
    #     struct {
    #     MsgHeader header;         // standard message header
    #     int32     responseFlags;  // bit vector - see details below
    #     int64     cursorID;       // cursor id if client needs to do get more's
    #     int32     startingFrom;   // where in the cursor this reply is starting
    #     int32     numberReturned; // number of documents in the reply
    #     document* documents;      // documents
    # }
    flags, pos = bson._get_int(data, 16)
    cursor_id, pos = bson._get_long(data, pos,as_class=None, tz_aware=False, uuid_subtype=3)
    starting_from, pos = bson._get_int(data, pos)
    number_returned, pos = bson._get_int(data, pos)
    try:
        o = bson.decode_all(data[pos:length])
    except Exception as e:
        o = []
        logger.exception("exception on bson decode in parse_reply")
        logger.info(repr(data[pos:length]))

    return Operation(operation=OP_REPLY, cursor_id=cursor_id,
                     starting_from=starting_from, number_returned=number_returned,
                     documents=o)

def parse_insert(data, length):
    #struct {
    #    MsgHeader header;             // standard message header
    #    int32     flags;              // bit vector - see below
    #    cstring   fullCollectionName; // "dbname.collectionname"
    #    document* documents;          // one or more documents to insert into the collection
    #}
    # flags
    # 0	ContinueOnError	If set, the database will not stop processing a bulk insert if one fails
    # (eg due to duplicate IDs). This makes bulk insert behave similarly to a series of single inserts,
    # except lastError will be set if any insert fails, not just the last one. If multiple errors occur,
    # only the most recent will be reported by getLastError. (new in 1.9.1)
    # 1-31	Reserved	Must be set to 0.
    flags, pos = bson._get_int(data, 16)
    continue_on_error = check_bit(flags, 0)
    collection, pos = bson._get_c_string(data, pos)
    try:
        o = bson.decode_all(data[pos:])
    except bson.InvalidBSON as e:
        o = []
        logger.exception("exception on bson decode")

    return Operation(operation=OP_INSERT,
                     collection=collection,
                     continue_on_error=continue_on_error,
                     documents=o)

def parse_get_more(data, length):
    #struct {
    #    MsgHeader header;             // standard message header
    #    int32     ZERO;               // 0 - reserved for future use
    #    cstring   fullCollectionName; // "dbname.collectionname"
    #    int32     numberToReturn;     // number of documents to return
    #    int64     cursorID;           // cursorID from the OP_REPLY
    #}
    zero, pos = bson._get_int(data, 16)
    collection, pos = bson._get_c_string(data, pos)
    numberToReturn, pos = bson._get_int(data, pos)
    cursorID, pos = bson._get_long(data, pos)

    return Operation(operation=OP_GET_MORE, collection=collection,numberToReturn=numberToReturn, cursorID=cursorID)

def parse_kill_cursors(data, length):
    #struct {
    #    MsgHeader header;            // standard message header
    #    int32     ZERO;              // 0 - reserved for future use
    #    int32     numberOfCursorIDs; // number of cursorIDs in message
    #    int64*    cursorIDs;         // sequence of cursorIDs to close
    #}
    zero, pos = bson._get_int(data, 16)
    number_of_cursor_ids, pos = bson._get_int(data, pos)
    cursor_ids = []
    for i in range(0, number_of_cursor_ids):
        cursor_id, pos = bson._get_long(data, pos)
        cursor_ids.append(cursor_id)
    return Operation(operation=OP_KILL_CURSORS, number_of_cursor_ids=number_of_cursor_ids, cursor_ids=cursor_ids)


parsers = {
    OP_UPDATE: parse_update,
    OP_QUERY: parse_query,
    OP_DELETE: parse_delete,
    OP_REPLY: parse_reply,
    OP_INSERT: parse_insert,
    OP_GET_MORE: parse_get_more,
    OP_KILL_CURSORS: parse_kill_cursors
}

def parse_data(data):
    # first 16 bytes is header
    header = data[:16]

    # struct MsgHeader {
    #     int32   messageLength; // total message size, including this
    #     int32   requestID;     // identifier for this message
    #     int32   responseTo;    // requestID from the original request
    #                            //   (used in reponses from db)
    #     int32   opCode;        // request type - see table below
    # }
    length = struct.unpack("<i", header[0:4])[0]
    request_id = struct.unpack("<i", header[4:8])[0]
    response_to = struct.unpack("<i", header[8:12])[0]
    op_code = struct.unpack("<i", header[12:16])[0]

    if op_code in parsers:
        op = parsers[op_code](data, length)
        op.request_id = request_id
        op.response_to = response_to
        logger.info("operation: %s", op)
    else:
        try:
            logger.info("operation: %s %s", opnames[op_code][0], request_id)
        except KeyError:
            logger.info("operation: %s %s", op_code, request_id)
            logger.debug(repr(data))
    return data

def get_length(data):
    length = struct.unpack("<i", data[0:4])[0]
    return length

class MongoProxy(StreamServer):

    def __init__(self, listener, mongo, **kwargs):
        StreamServer.__init__(self, listener, **kwargs)
        self.mongo = mongo

    def handle(self, source, address):
        logger.info('%s:%s accepted', *address[:2])

        try:
            mongo_connection = create_connection(self.mongo)
        except IOError as ex:
            logger.exception('%s:%s failed to connect to %s:%s:', address[0], address[1], self.mongo[0], self.mongo[1])
            return

        gevent.spawn(forward_upstream, source, mongo_connection)
        gevent.spawn(forward_downstream, source, mongo_connection)

    def close(self):
        if self.closed:
            sys.exit('Multiple exit signals received - aborting.')
        else:
            logger.info('Closing listener socket')
            StreamServer.close(self)


def split_data(buffer, expected_length):
    if len(buffer) == expected_length:
        return (buffer, "")
    if len(buffer) > expected_length:
        return (buffer[0:expected_length], buffer[expected_length:])
    if len(buffer) < expected_length:
        return ("", buffer)

def get_data(socket):
    buffer = ""
    while True:
        buffer += socket.recv(1024)
        if not buffer:
            break
        while True:
            if not buffer:
                break
            if len(buffer)<4:
                break
            data, remainder = split_data(buffer, get_length(buffer))
            if data:
                yield data
                buffer = remainder
            else:
                break

def forward_(source, dest):
    source_address = '%s:%s' % source.getpeername()[:2]
    dest_address = '%s:%s' % dest.getpeername()[:2]
    for data in get_data(source):
        logger.info('received %s -> %s:', source_address, dest_address)
        parse_data(data)
        if not dest.closed:
            dest.sendall(data)

def forward_upstream(client, mongo):
    """
    forward from upstream mongo to client
    """
    try:
        forward_(mongo, client)
    finally:
        logger.info("closing client connection %s:%s" % client.getpeername()[:2])
        mongo.close()

def forward_downstream(client, mongo):
    """
    forward from client to mongo upstream
    """
    try:
        forward_(client, mongo)
    finally:
        logger.info("closing upstream connection %s:%s" % mongo.getpeername()[:2])
        client.close()

def parse_address(address):
    try:
        hostname, port = address.rsplit(':', 1)
        port = int(port)
    except ValueError:
        sys.exit('Expected HOST:PORT: %r' % address)
    return gethostbyname(hostname), port


def run_proxy():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo', help='mongo server eg: 127.0.0.1:27017', default="127.0.0.1:27017")
    parser.add_argument('--listen', help='listen eg: 127.0.0.1:29017', default="127.0.0.1:29017")
    parser.add_argument('--loglevel', help="log level, eg:DEBUG, CRITICAL etc.", default="DEBUG")

    args = parser.parse_args()

    logger.setLevel(logging.getLevelName(args.loglevel))

    source = parse_address(args.listen)
    mongo = parse_address(args.mongo)
    server = MongoProxy(source, mongo)
    logger.info('Starting proxy %s:%s -> %s:%s', *(server.address[:2] + mongo))
    gevent.signal(signal.SIGTERM, server.close)
    gevent.signal(signal.SIGINT, server.close)
    server.start()
    gevent.wait()


if __name__ == '__main__':
    run_proxy()