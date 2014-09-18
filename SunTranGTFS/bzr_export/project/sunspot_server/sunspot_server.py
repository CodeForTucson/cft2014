#!/usr/bin/env python3
#
# sunspot_server.py
#
# main server file for the sunspot app / server backend
#
# Mark Grandi - Aug 22, 2014
#

###################
# cherrypy stuff
###################
# TODO: why do i need these imports? find out later!
import sys
sys.stdout = sys.stderr

import hashlib
import atexit
import threading
import cherrypy
from cherrypy.lib.static import serve_file


###################
# library imports
###################

import os.path
import os
import zlib
import requests
import zipfile
import csv
import io
import arrow
from contextlib import closing
import yaml
import lzma
import pathlib
import json

###################
# project imports 
###################

# TODO: HACK or else it won't import our classes!
if "/var/www/sunspot" not in sys.path:
    sys.path.append("/var/www/sunspot")
from constants import Constants
from server_database import CherrypyServerDatabase, ServerDatabaseEnums


###################
# protocol buffer imports
###################

from sunspot_messages_pb2 import SunspotMessages
from gtfs_realtime_pb2 import FeedMessage
import google.protobuf.message # for DecodeError exception


class Root:
    ''' main class / cherrypy application '''

    # let cherrypy know that we are exposing this class to the web to be called
    exposed = True
    
    def __init__(self):
        ''' constructor'''

        pass

    def _setApp(self, app):
        ''' hackish method to make it so this object has a reference to the applications Logger object
        and so we can access its config dictionary as well.'''

        # TODO HACK: maybe just save instance of the logger instead of the app, we probably have
        # a recursive dependency here
        self.app = app
        self.logger = self.app.log
        self.logger.error_log.setLevel("DEBUG")

        # self.logger.error(str(app.config))
        # self.logger.error(app.config["/"]["constants_yaml"])

        self.constants = Constants(app.config["/"]["constants_yaml"])


    def _handleError(self, errCode, logMsg, errMsg, httpErrCode=400):
        ''' helper method to construct a error protobuf message, sets the http status code to 
        be 400, and then we return the serialized protobuf object. So call 'return _handleError()'

        @param errCode - one of the ServerErrorType enumerations
        @param logMsg - the message we log to our own log
        @param errMsg - the human readable message thats given with the error code 
        @param httpErrCode - the http error code to set the request headers to
        @return the serialized protobuf object (bytes)'''

        # create the ServerResponse that has the error message with it
        sunMsg = SunspotMessages.ActualSunspotMessage()
        sunMsg.timestamp = self._getTimestamp()
        sunMsg.message_type = SunspotMessages.ActualSunspotMessage.SERVER_RESPONSE
        sunMsg.server_response_message.data_we_contain = SunspotMessages.ServerResponse.CONTAINS_ERROR
        sunMsg.server_response_message.error_data.error_code = errCode
        sunMsg.server_response_message.error_data.error_message = errMsg

        self.logger.error(logMsg)

        cherrypy.response.status = httpErrCode

        return sunMsg.SerializeToString()


    class CompressorNone:
        '''compresion object that we return from _getCompressorObj, this is for COMPRESSION_NONE'''

        def __init__(self, logger):
            ''' constructor 
            @param logger - a logger object'''

            self.logger = logger
            self.compressionType = SunspotMessages.COMPRESSION_NONE


        def compress(self, bytesToCompress):
            ''' compresses the data with COMPRESSION_NONE
            @param bytesToCompress - the bytes to compress, duh
            @return the compressed bytes'''

            return bytesToCompress


    class CompressorLzmaXz:
        '''compresion object that we return from _getCompressorObj, this is for COMPRESSION_LZMA_XZ'''

        def __init__(self, logger):
            ''' constructor
            @param logger - a logger object'''

            self.logger = logger
            self.compressionType = SunspotMessages.COMPRESSION_LZMA_XZ

        def compress(self, bytesToCompress):
            '''@param bytesToCompress - the bytes to compress, duh
            @return the compressed bytes'''

            self.logger.error("CompressorLzmaXz: beginning compression")
            zeBytes =  lzma.compress(bytesToCompress, format=lzma.FORMAT_XZ)
            self.logger.error("CompressorLzmaXz: finished compression")
            return zeBytes



    def _getCompressorObj(self, reqCompressionList):
        ''' helper method to take in the requested_compression repeated field from the 
        ServerQuery and return a object that will compress it once 'compress()' is called
        @param reqCompressionList - the repeated CompressionType field
        @return a object to compress the data
        '''

        objToReturn = self.CompressorNone
        
        self.logger.error("_getCompressorObj(): The compression list we got: {}".format(str(reqCompressionList)))

        for iterCompression in reqCompressionList:

            # if its COMPRESSION_NONE, then we skip it as its already 
            # the default and every message should have this.
            if iterCompression == SunspotMessages.COMPRESSION_NONE:
                continue

            elif iterCompression == SunspotMessages.COMPRESSION_LZMA_XZ:
                self.logger.error("_getCompressorObj(): Returning CompressorLzmaXz")
                objToReturn = self.CompressorLzmaXz
                break
            else:
                self.logger.error("WARNING: unknown compression type? {}".format(iterCompression))

        return objToReturn(self.logger)



    def _getTimestamp(self):
        ''' helper method to get a timestamp'''

        return str(arrow.utcnow().timestamp)


    def GET(self):
        ''' called when we recieve a GET request'''
        return "hello"


    @cherrypy.tools.accept(media='application/octet-stream') # we only accept application/octet-stream content-types
    def POST(self):
        ''' called when we recieve a POST request'''


        sbObj = CherrypyServerDatabase(self.constants.CONFIG_DB_PATH, self.logger)

        self.logger.error("Current dir: {}".format(os.getcwd()))
        self.logger.error("application config: {}".format(application.config))
        self.logger.error("process request: {}".format(cherrypy.request.process_request_body))
        self.logger.error("method: {}".format(cherrypy.request.method))

        data = cherrypy.request.body.read()
        self.logger.error("The data is: {}".format(data))


        # read the data that the client sent us
        clientObj = SunspotMessages.ActualSunspotMessage()
        try:
            clientObj.ParseFromString(data)
        except google.protobuf.message.DecodeError as e:            
            return self._handleError(SunspotMessages.ServerError.INVALID_PROTOBUF_MSG, 
                "ERROR: failed to decode SunspotMessages from data: '{}'".format(e),
                "couldn't decode protobuf, error: {}".format(e))


        # ok, what does the client want?
        msgType = clientObj.message_type

        if msgType == SunspotMessages.ActualSunspotMessage.SERVER_QUERY:

            # make sure that we have the actual data for this
            if not clientObj.HasField("server_query_message"):
                return self._handleError(SunspotMessages.ServerError.INVALID_REQUEST, 
                    "ERROR: got a ActualSunspotMessage.message_type of SERVER_QUERY but we have no " 
                        + "'sever_query_message' data! protobuf: {}".format(str(clientObj)),
                    "got SERVER_QUERY message_type but there is no 'server_query_message' data!")


            ###################
            # ok we have that, see what they are asking for.
            ###################
            askingFor = clientObj.server_query_message.asking_for

            if askingFor == SunspotMessages.ServerQuery.NEED_FULL_DB:

                # give the user the full db
                fullDbProtoObj = SunspotMessages.ActualSunspotMessage()
                fullDbProtoObj.timestamp = self._getTimestamp()
                fullDbProtoObj.message_type = SunspotMessages.ActualSunspotMessage.SERVER_RESPONSE

                respMsg = fullDbProtoObj.server_response_message

                respMsg.data_we_contain = respMsg.CONTAINS_FULL_DB
                respMsg.version_of_db = str(0) # TODO FIX

                compressorObject = self._getCompressorObj(clientObj.server_query_message.requested_compression)

                # need to ask our database what the full path for the latest db is.
                latestDbTime = None
                latestDbPath = None
                try:
                    latestDbTime = sbObj[ServerDatabaseEnums.KEY_LAST_DOWNLOAD_TIME]

                    latestDbPath = sbObj.getWithPrefix(ServerDatabaseEnums.PREFIX_DATABASE_TIME_TO_LOCATION, [latestDbTime])

                except Exception as e:
                    return self._handleError(SunspotMessages.ServerError.SERVER_ERROR, "ERROR: got KeyError when reading from "
                        + " ServerDatabase when trying to get the path to the latest database: {}".format(e), 
                        "Server encountered an error", 500)

                with open(latestDbPath, "rb") as f:
                    respMsg.actual_data = compressorObject.compress(f.read())

                respMsg.compression_type = compressorObject.compressionType

                cherrypy.response.headers['Content-Type'] = "application/octet-stream"
                return fullDbProtoObj.SerializeToString()

            elif askingFor == SunspotMessages.ServerQuery.NEED_PATCH:
                pass

            else:
                return self._handleError(SunspotMessages.ServerError.SERVER_DOESNT_RECOGNIZE,
                "ERROR: got a ActualSunspotMessage.server_query_message.asking_for that we do not recognize,"
                    + " is our protobuf class out of date??? protobuf: {}".format(str(clientObj)),
                "Server doesn't recognize the ActualSunspotMessage.server_query_message.asking_for enum", 
                500)

            ###################


        elif msgType == SunspotMessages.ActualSunspotMessage.SERVER_RESPONSE:
            pass

        else:

            return self._handleError(SunspotMessages.ServerError.SERVER_DOESNT_RECOGNIZE,
                "ERROR: got a ActualSunspotMessage.message_type that we do not recognize,"
                    + " is our protobuf class out of date??? protobuf: {}".format(str(clientObj)),
                "Server doesn't recognize the ActualSunspotMessage.message_type enum", 
                500)
            
            



        self.logger.error("the object was: {}".format(str(clientObj)))

        return "hello"


class TestZip():
    ''' cherrypy application that just returns a zip file depending on the GET's num parameter
     so we can pretend that we are downloading different versions of the GTFS data.'''

    # let cherrypy know that we are exposing this class to the web to be called
    exposed = True


    def __init__(self):
        ''' constructor'''

        self.logger = None
        self.zipFiles = [
            "gtfs_zip_1route.zip",
            "gtfs_zip_5route.zip",
            "gtfs_zip_10route.zip",
            "gtfs_zip_20route.zip",
            "gtfs_zip_30route.zip",
            "gtfs_zip_40route.zip",
            "gtfs_zip_49route.zip"

        ]

        self.dbStartTime = 1408657604



    def GET(self, num=None, length=None):
        ''' called when we recieve a GET request'''

        if length:
            return str(len(self.zipFiles))

        if num is None or int(num) >= len(self.zipFiles):
            raise cherrypy.HTTPError(401)

        tmpZipFilePath = pathlib.Path("/var/www/sunspot/",self.zipFiles[int(num)])

        modtime = (self.dbStartTime + (int(num) * 5))


        # make it so that the modified times are different so we get different times / versions when we download them!
        # NOTE: THE ZIP FILES MUST BE OWNED by www-data for this call to work!
        os.utime(str(tmpZipFilePath), times=(modtime, modtime))

        self.logger.error("Testzip: num is {}, serving {}".format(num, tmpZipFilePath))
        return serve_file(str(tmpZipFilePath), "application/zip", str(tmpZipFilePath))


class TmpFindBus():
    ''' simple cherrypy application that just returns json of a bus's coordinates that they specify
    '''

    # let cherrypy know that we are exposing this class to the web to be called
    exposed = True


    def __init__(self):
        ''' constructor'''
        self.logger = None


    def GET(self, busNum, milliSinceEpoch):

        cherrypy.response.headers['Content-Type'] = "application/json"

        actualDate = arrow.get(int(milliSinceEpoch) / 1000)

        self.logger.error("[{}]: bus number is {}".format(actualDate.isoformat(), busNum))


        resp = requests.get("http://suntran.com/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb")


        try:
            protoObj = FeedMessage.FromString(resp.content)
        except google.protobuf.message.DecodeError as e:  
            raise cherrypy.HTTPError(401)

        for iterEnt in protoObj.entity:
            if iterEnt.HasField("vehicle"):
                if iterEnt.vehicle.vehicle.label == busNum:
                    ts = arrow.now().isoformat()
                    returnStr = json.dumps({
                        "vehicle": busNum, 
                        "lat": iterEnt.vehicle.position.latitude, 
                        "lon": iterEnt.vehicle.position.longitude,
                        "timestamp": ts})
                    self.logger.error("\tFound bus, returning lat: {}, lon: {}, time: {}".format(
                        iterEnt.vehicle.position.latitude, iterEnt.vehicle.position.longitude, ts ))
                    return returnStr.encode("utf-8")

        # if we get here we haven't found the bus
        self.logger.error("\tcouldn't find bus with number {}".format(busNum))
        raise cherrypy.HTTPError(401)

        # return "{'hello': 'bob'}".encode("utf-8")

# config object we pass to cherrypy.Application()
config = {
    '/':
    {
        'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
        "log.error_file": "/var/www/sunspot_error.log",
        "constants_yaml": "/var/www/sunspot/constants_config.yaml"
    } ,
    "/testzip":
    {
        'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
        "log.error_file": "/var/www/sunspot_error.log",
        "constants_yaml": "/var/www/sunspot/constants_config.yaml"
    },
    "tmpFindBus": {

        'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
        "log.error_file": "/var/www/tmpfindbus_error.log",

    }
}


# server setup
root = Root()
root.testzip = TestZip()
root.tmpFindBus = TmpFindBus()
application = cherrypy.Application(root, script_name="/sunspot", config=config)
root.testzip.logger = application.log
root.tmpFindBus.logger = application.log
root._setApp(application)
