#
#
# dead simple server to provide access to the leveldb database from multiple processes
# as each process can just connect to this server
# based off the protocol example in the python documentation
#
# Mark Grandi - Aug 27, 2014
#


import asyncio
import json
import logging, logging.handlers
import argparse, sys

# third party libraries

import yaml
import leveldb


# project imports

from constants import Constants

class LeveldbServer(asyncio.Protocol):
    ''' the server class itself (that subclasses Protocol)'''

    # static members
    dbPath = None
    db = None

    def __init__(self):
        ''' constructor'''
        self.lg = logging.getLogger("LeveldbServer")
        self.lg.debug("LeveldbServer object created")
        self.name = "unnamed"

    def __del__(self):
        ''' destructor'''
        self.lg.debug("LeveldbServer destroyed")

    def _peernameTupleToName(self, peernameTuple):
        ''' turns a tuple like ('127.0.0.1', "57276") to a string name
        '''

        return peernameTuple[0].replace(".", "-") + ":" + str(peernameTuple[1])

    def connection_made(self, transport):
        ''' called when a client makes a connection to this server
        @param transport - The transport argument is the transport representing the connection. 
            You are responsible for storing it somewhere (e.g. as an attribute) if you need to.'''

        self.lg = self.lg.getChild(self._peernameTupleToName(transport.get_extra_info('peername')))

        peername = transport.get_extra_info('peername')
        self.lg.info('connection from {}'.format(peername))
        self.transport = transport

    def data_received(self, data):
        ''' called when we recieve data from a client
        @param data - the data recieved (as bytes)'''

        self.lg.debug('data received: {}'.format(data.decode()))

        jData = json.loads(data.decode("utf-8"))
        if jData["type"] == "get":

            value = None
            try:
                value = LeveldbServer.db.Get(jData["key"].encode("utf-8")).decode("utf-8")
            except:
                self.transport.write( json.dumps({"return": None}).encode("utf-8"))
                return

            # everything is ok
            self.transport.write( json.dumps({"return": value}).encode("utf-8"))

        else:

            LeveldbServer.db.Put(jData["key"].encode("utf-8"), jData["value"].encode("utf-8"))
            self.transport.write("ok".encode("utf-8"))

    def eof_received(self):

        self.lg.debug("eof recieved")
        return False # cause the transport to close itself

    def connection_lost(self, exc):
        ''' called when the client disconnects from the server'''

        self.lg.info("lost connection to {}".format(self.transport.get_extra_info("peername")))

        #self.transport.write(data)

        # close the socket
        #self.transport.close()




def isYamlType(stringArg):
    ''' helper method for argparse that sees if the argument is a valid yaml file
    @param stringArg - the argument we get from argparse
    @return the @stringArg if it is indeed a valid yaml file, or raises ArgumentTypeError if its not. '''

    try:
        with open(stringArg, encoding="utf-8") as f:
            test=yaml.safe_load(f)
        
    except yaml.YAMLError as e:
        if hasattr(e, "problem_mark"):
            mark=e.problem_mark
            raise argparse.ArgumentTypeError("Problem parsing yaml: error was at {}:{}".format(mark.line+1, mark.column+1))
        else:
            raise argparse.ArgumentTypeError("Problem parsing yaml! {}".format(e))
    except OSError as e2:
        raise argparse.ArgumentTypeError("error reading file at {}, error was {}".format(stringArg, e2))
    except Exception as e3:
        raise argparse.ArgumentTypeError("general error: {}".format(e3))

    # if it succeeds, then return the yaml filepath
    return stringArg

def startLeveldbServer(args):
    '''Starts a server to serve a levelDB database on localhost
    @param args - the namespace object we get from argparse.parse_args()
    '''

    constantsObj = Constants(args.configYaml)

    serverRootLogger = logging.getLogger("LeveldbServer")

    # see what handler we are using, either stream or rotating file
    if constantsObj.SERVERDATABASE_LOGGING_OUTPUT_TO_CONSOLE:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.handlers.RotatingFileHandler(
            filename=constantsObj.SERVERDATABASE_LOGGING_OUTPUT_NAME,
            mode="a", 
            maxBytes=constantsObj.SERVERDATABASE_LOGGING_MAXBYTES,
            backupCount=constantsObj.SERVERDATABASE_LOGGING_NUMBACKUPS,
            encoding="utf-8")

    logging.basicConfig(level=constantsObj.SERVERDATABASE_LOGGING_LEVEL, 
        format="%(asctime)s %(name)-35s %(levelname)-8s: %(message)s",
        handlers=[handler])


    serverRootLogger.info("loggers have been configured")
    serverRootLogger.info("\tusing handler: {}".format(handler))


    loop = asyncio.get_event_loop()
    coro = loop.create_server(LeveldbServer, '127.0.0.1', 8888)

    # create class member for the database
    LeveldbServer.dbPath = constantsObj.CONFIG_DB_PATH
    LeveldbServer.db = leveldb.LevelDB(LeveldbServer.dbPath)


    server = loop.run_until_complete(coro)
    serverRootLogger.info('serving on {}'.format(server.sockets[0].getsockname()))
    serverRootLogger.info("using database at {}".format(LeveldbServer.dbPath))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("exit")
    finally:
        serverRootLogger("closing server and loop")
        server.close()
        loop.close()



if __name__ == "__main__":
    # if we are being run as a real program

    parser = argparse.ArgumentParser(description="Starts a server to serve a levelDB database on localhost", 
    epilog="Copyright Aug 27, 2014 Mark Grandi")

    parser.add_argument('configYaml',  type=isYamlType, help="the YAML config file that is meant for the Constants class")

    argparseLg = logging.getLogger("argparse")

    try:
        startLeveldbServer(parser.parse_args())
    except Exception as e:
        argparseLg.exception("uncaught exception: {}".format(e))
        logging.shutdown()
        sys.exit(1)

    # exiting normally
    argparseLg.info("Shutting down normally")
    logging.shutdown()



