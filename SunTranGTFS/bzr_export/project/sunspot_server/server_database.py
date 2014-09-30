import leveldb
import logging
import socket
import arrow
from leveldb_server_messages_pb2 import LeveldbServerMessages

class ServerDatabaseEnums:
    ''' this class will hold various full keys or 'prefixes' for leveldb keys
    that we use to get information from the leveldb database'''


    KEY_LAST_DOWNLOAD_TIME = "last_download_time"
    PREFIX_DATABASE_TIME_TO_LOCATION = "t:{}"
    KEY_DB_PATCH_LETTER = "p:"
    PREFIX_DB_PATCH = "{}{}_{}".format(KEY_DB_PATCH_LETTER, "{}", "{}")




class ServerDatabase:
    ''' class that wraps around a LevelDB database that holds various stuff about the state of our server,
    what databases we have, what patches we have, what is the latest version of the GTFS data, etc.

    You should pass in various ServerDatabaseEnums class values to the methods in this class, to avoid magic numbers/values! 
    '''



    def __init__(self, ipAddr, port, logger=None):
        '''constructor
        @param ipAddr - the ip address as a string of the leveldb_server that we want to connect to
        @param port - the port as a string of the leveldb_server that we want to connect to
        @param logger - a logger object'''

        # self.db = leveldb.LevelDB(databasePath)
        # self.dbFilePath = databasePath


        self.lg = logger

        #self._log("Opening database at {}".format(databasePath))

        self.socket = socket.create_connection((ipAddr, port))

    def _socketSend(self, msg):
        ''' method to handle sending / writing data to the socket that is connected to the 'leveldb_server'.
        Since a socket can not send all of the data that you tell it to, we need to keep trying until you
        have sent all the data or get back '0' which is an error. Taken from https://docs.python.org/3/howto/sockets.html
        We prefix the 'length' of the message that we are sending to the actual bytes that we write to the socket so the 
        server knows how much to read, in case it also doesn't read the entire message at once.

        @param msg - the bytes to send'''

        totalsent = 0

        # appends a 2 byte 'size prefix' to the data we send to the client
        actualMsg = len(msg).to_bytes(2, "big") + msg 
        msgLen = len(actualMsg)
        while totalsent < msgLen:
            sent = self.socket.send(actualMsg[totalsent:])
            if sent == 0:
                self._log("Got 0 bytes on socket.send(), connection is broken!", severity="ERROR")
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent


    def _socketRecvWithSizePrefix(self):
        ''' helper method that helps reading data that is prefixed with the 2 byte size prefix.
        It basically calls _socketRecv(2), to get the size prefix, and then _socketRecv(SIZE_PREFIX) to get the 
        rest of the data and then returns it'''

        # get the size
        sizePrefix = int.from_bytes(self._socketRecv(2), "big")

        # then read 'SIZE' bytes and return it
        return self._socketRecv(sizePrefix)

    def _socketRecv(self, msgLen):
        ''' method to handle reading / receiving data from the socket that is connected to the 'leveldb_server'.
        Since a socket can read only part of the data that was sent, we need to keep trying until we have read
        all of the data that was sent. We handle the 'length' by having 2 bytes that are prefixed to all of the messages
        that the leveldb_server sends us, which is the remaining length of the message, so we know when we have got it all.
        Taken from https://docs.python.org/3/howto/sockets.html

        @param msgLen - the length of the message that we are receiving
        @return the bytes that we received''' 
        chunks = []
        bytes_recd = 0
        while bytes_recd < msgLen:
            #print("\treading, msgLen is {}".format(msgLen))
            chunk = self.socket.recv(min(msgLen - bytes_recd, 2048))
            if chunk == b'':
                self._log("Got 0 bytes on socket.recv(), connection is broken!", severity="ERROR")
                raise RuntimeError("socket connection broken")
            #print("\t\tgot chunk: {}".format(chunk))
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)

        #print("\tdone reading")
        return b''.join(chunks)

    def _log(self, msg, severity=logging.DEBUG):
        ''' logs a message under the DEBUG level, overridden by subclasses
        @param msg - the message to log
        @param severity - what severity to log this message under, defaults to DEBUG
        '''

        if self.lg:
            self.lg.log(severity, msg)

    def _createProtoQuery(self):
        ''' helper method that creates the LeveldbServerMessages.ActualData object for us, sets the timestamp
        and the type, and then returns it for us

        this is for a ServerQuery

        @return a ActualData protobuf object'''

        protoObj = LeveldbServerMessages.ActualData()
        protoObj.timestamp = arrow.now().timestamp # int64
        protoObj.type = LeveldbServerMessages.ActualData.QUERY
        
        return protoObj

    def _isProtoComplete(self, protoObj):
        ''' helper method that sees if a protobuf object is complete
        and we can send it. TODO this should probably be a decorator...

        @param protoObj - the protobuf object to check

        @return if the protoObj is not complete, then we raise an exception, if not, we return
            the protoObj serialized as bytes'''

        if protoObj.IsInitialized():
            return protoObj.SerializeToString()
        else:
            self._log("Protobuf object is not complete!", severity=logging.ERROR)
            raise ValueError("protobuf object is not complete!")


    def __getitem__(self, key):
        '''implementation of obj[item]

        @param key - a string
        @return A STRING'''

        self._log("retrieving value from key '{}'".format(key))

        #return self.db.Get(key.encode("utf-8")).decode("utf-8")
        
        protoObj = self._createProtoQuery()
        query = protoObj.query

        # set the key, encoded into bytes
        query.key = key.encode("utf-8")
        # set the operation we want the server to do
        query.type = LeveldbServerMessages.ServerQuery.GET

        # send it
        self._socketSend(self._isProtoComplete(protoObj))
        self._log("Sending proto message: {}".format(str(protoObj)))

        # wait for response and return it, we get back bytes of a protobuf object
        resultBytes = self._socketRecvWithSizePrefix()
        self._log("got back bytes: {}".format(resultBytes))

        # figure out if the server sent us a KeyError or a real value
        protoResult = LeveldbServerMessages.ActualData.FromString(resultBytes)
        self._log("recieved protoMessage: {}".format(str(protoResult)))
        resp = protoResult.response

        if resp.type == LeveldbServerMessages.ServerResponse.GET_PRODUCED_KEYERROR:
            # key error happened, raise the exception as normal
            self._log("Got keyerror from server!", severity=logging.ERROR)
            raise KeyError(resp.query_ran.key.decode("utf-8"))

        elif resp.type == LeveldbServerMessages.ServerResponse.GET_RETURNED_VALUE:
            # return the value
            self._log("got normal value back from server, {}".format(resp.returned_value))
            return resp.returned_value.decode("utf-8")
        else:
            self._log("Didn't get an expected ServerResponse type! got: {}".format())
            raise Exception("Unexpected ServerResponse")




    def __setitem__(self, key, value):
        '''implementation of obj[item] = something

        @param key - a string
        @param value - a string
        '''

        self._log("Setting key: '{}' , value: '{}'".format(key, value))

        #self.db.Put(key.encode("utf-8"), value.encode("utf-8"))

        protoObj = self._createProtoQuery()
        query = protoObj.query

        # set the key, encoded into bytes
        query.key = key.encode("utf-8")
        query.value = value.encode("utf-8")

        # set the operation we want the server to do
        query.type = LeveldbServerMessages.ServerQuery.SET

         # send it
        self._socketSend(self._isProtoComplete(protoObj))
        self._log("Sending proto message: {}".format(str(protoObj)))

        # wait for response and return it, we get back bytes of a protobuf object
        resultBytes = self._socketRecvWithSizePrefix()
        self._log("got back bytes: {}".format(resultBytes))

         # figure out if the server sent us a KeyError or a real value
        protoResult = LeveldbServerMessages.ActualData.FromString(resultBytes)
        self._log("recieved protoMessage: {}".format(str(protoResult)))
        resp = protoResult.response

        if resp.type == LeveldbServerMessages.ServerResponse.SET_SUCCESSFUL:

            self._log("set was successful!")

        else:
            raise Exception("Got a unsuccessful server message for set! {}".format(resp.type))



    def __delitem__(self, key):
        '''implementation of del obj["item"]

        @param key - a string
        '''

        self._log("Deleting key: '{}'".format(key))
        #self.db.Delete(key.encode("utf-8"))


        protoObj = self._createProtoQuery()
        query = protoObj.query

        # set the key, encoded into bytes
        query.key = key.encode("utf-8")

        # set the operation we want the server to do
        query.type = LeveldbServerMessages.ServerQuery.DELETE

        # send it
        self._socketSend(self._isProtoComplete(protoObj))
        self._log("Sending proto message: {}".format(str(protoObj)))

        # wait for response and return it, we get back bytes of a protobuf object
        resultBytes = self._socketRecvWithSizePrefix()
        self._log("got back bytes: {}".format(resultBytes))

         # figure out if the server sent us a KeyError or a real value
        protoResult = LeveldbServerMessages.ActualData.FromString(resultBytes)
        self._log("recieved protoMessage: {}".format(str(protoResult)))
        resp = protoResult.response

        if resp.type == LeveldbServerMessages.ServerResponse.DELETE_SUCCESSFUL:
            self._log("Delete was successful!")

        else:
            raise Exception("Got an unsuccessful server message for delete! {}".format(resp.type))



    def __del__(self):
        ''' destructor'''

        # according to the source code for py-leveldb, when the LevelDB object gets deallocated it
        # closes the database normally. Even in c++ you don't really close the database, you just call del *db
        # and it closes automatically

        try:
            self.socket.shutdown(socket.SHUT_RDWR) # tell server we are not listening or sending any more data
            self.socket.close()

        except Exception as e:
            self._log("Error in deconstructor: {}".format(e))
            pass

        self._log("ServerDatabase.__del__() called")


    def setWithPrefix(self, key, formatEntries, value):
        ''' sets a @value with the @key formatted with @formatEntries (should be something from ServerDatabaseEnums) 

        @param key - the key to set in the leveldb database, that has some format markers ({})
        @param formatEntries - an iterable to use when we call .format() on @key
        @param value - the value to set as the value for @key in the leveldb database
        '''

        # don't do any encoding or decoding, the __setitem__ method does that

        self[key.format(*formatEntries)] = value

    def getWithPrefix(self, key, formatEntries):
        ''' returns a @value with the @key formatted with @formatEntries (should be something from ServerDatabaseEnums )

        @param key - the key to use to retrieve a value from the leveldb database, that has some format markers ({})
        @param formatEntries - an iterable to use when we call .format() on @key
        @return a STRING
        '''

        # don't do any encoding or decoding, the __getitem__ method does that
        return self[key.format(*formatEntries)]

    def getGeneratorWithPrefix(self, key, formatEntries):
        ''' a generator that yields entries from RangeIter as long as the keys startwith the key.format(*formatEntries)

        with the @key formatted with @formatEntries (should be something from ServerDatabaseEnums )

        pass in a iterable of empty strings if you are searching by prefix (like you have c:1, c:2, c:3 and you 
            want to search by 'c:' )

        @param key - the key to use to start the leveldb search from, that has some format markers ({})
            will also be used to see if the keys the database returns start with this string (after format is called),
            if they don't startwith(@key), then we stop the iteration
        @param formatEntries - an iterable we use when we call .format() on @key
        @return a GENERATOR that returns a two-tuple of bytearrays that you NEED TO CALL DECODE() ON
        '''
        #self.lg.debug("key: {}, formatEntries: {}".format(key, formatEntries))

        thekey = key.format(*formatEntries)

        for iterResult in self.db.RangeIter(thekey.encode("utf-8")):
            if iterResult[0].decode("utf-8").startswith(thekey):
                yield iterResult
            else:
                break


    def deleteAllInRange(self, rangeStr, formatEntries):
        ''' deletes all the keys that we get back from a RangeIter() call with the
        rangeStr parameter

        rangeStr will be formatted with @formatEntries (should be something from ServerDatabaseEnums)

        @param rangeStr - the string key to use to start the leveldb search from (to delete), that has some format markers ({})
            will also be used to see if the keys the database returns start with this string (after format is called),
            if they don't startwith(@rangeStr), then we stop the iteration
        @param formatEntries - an iterable we use when we call .format() on @rangeStr
        '''

        toDelList = list()
        for iterResult in self.getGeneratorWithPrefix(rangeStr, formatEntries):
            toDelList.append(iterResult[0])

        for iterEntry in toDelList:
            self.lg.debug("Deleting key {}".format(iterEntry))
            self.db.Delete(iterEntry)

class CherrypyServerDatabase (ServerDatabase):
    ''' subclass of ServerDatabase, the only reason we do this
    is because cherrypy's LogManager is not a logging.logger so we have to do 
    something different in order to log'''


    def _log(self, msg, severity=logging.DEBUG):
        ''' overriding the '_log' method, which uses the cherrypy LogManager, which is a wrapper
        around a logging.Logger object, so we always log using error(), but we can specify a custom 
        prefix and severity

        @param msg - the message to log
        @param severity - what severity to log this message under, defaults to DEBUG'''

        self.lg.error(msg, "ServerDatabase", severity=severity)







