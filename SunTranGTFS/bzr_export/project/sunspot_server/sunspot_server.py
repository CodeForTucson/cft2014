
# TODO: why do i need these imports? find out later!
import sys
sys.stdout = sys.stderr

import hashlib
import atexit
import threading
import cherrypy

#####################################################

# TODO: HACK or else it won't import the pb2 class
if "/var/www/sunspot" not in sys.path:
    sys.path.append("/var/www/sunspot")
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

from constants import Constants
from server_config import ServerConfig

from cherrypy.lib.static import serve_file



cherrypy.log(str(sys.path))

from sunspot_messages_pb2 import SunspotMessages
import google.protobuf.message # for DecodeError exception


class Root:

    exposed = True
    
    

    def __init__(self):

        pass

    def _setApp(self, app):

        # TODO HACK: maybe just save instance of the logger instead of the app, we probably have
        # a recursive dependency here
        self.app = app
        self.logger = self.app.log

        self.logger.error(str(app.config))
        self.logger.error(app.config["/"]["constants_yaml"])

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


        def __init__(self, logger):
            '''compresion object that we return from _getCompressorObj, this is for COMPRESSION_NONE'''

            self.logger = logger
            self.compressionType = SunspotMessages.COMPRESSION_NONE


        def compress(self, bytesToCompress):
            ''' compresses the data with COMPRESSION_NONE
            @param bytesToCompress - the bytes to compress, duh
            @return the compressed bytes'''

            return bytesToCompress


    class CompressorLzmaXz:

        def __init__(self, logger):
            '''compresion object that we return from _getCompressorObj, this is for COMPRESSION_LZMA_XZ'''

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
        return "hello"


    @cherrypy.tools.accept(media='application/octet-stream')
    def POST(self):

        configObj = ServerConfig(self.constants.CONFIG_FILE_PATH)

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

                with open("/var/www/sunspot/db.sqlite", "rb") as f:
                    respMsg.actual_data = compressorObject.compress(f.read())

                respMsg.compression_type = compressorObject.compressionType

                cherrypy.response.headers['Content-Type'] = "application/octet-stream"
                return fullDbProtoObj.SerializeToString()

            elif askingFor == SunspotMessages.ServerQuery.NEED_PATCH:
                pass

            else:
                self._handleError(SunspotMessages.ServerError.SERVER_DOESNT_RECOGNIZE,
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

    exposed = True


    def __init__(self):

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
    }}

root = Root()
root.testzip = TestZip()
application = cherrypy.Application(root, script_name="/sunspot", config=config)
root.testzip.logger = application.log
root._setApp(application)
