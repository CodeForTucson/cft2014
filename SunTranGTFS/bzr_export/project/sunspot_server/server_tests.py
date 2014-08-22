#
# Script to help test sunspot_server.py
#
# written by Mark Grandi - Aug 8 2014
#


import unittest
import sys
import requests
import pathlib
from contextlib import closing

###################
# IMPORTING STUFF
###################
try:
    import constants
    from constants import Constants
    import arrow
except ImportError as e:
    sys.exit("Unable to import some sunspot_server related classes...error: '{}'".format(e))

# make sure we can import the protoc generated classes
try:
    from sunspot_messages_pb2 import SunspotMessages
except ImportError as e:
    sys.exit("Could not import 'sunspot_messages_pb2 or the protobuf libraries... have you generated " 
        + "the python class from the .proto file and or installed 'protobuf-py3' from pip? error: '{}' "
        .format(e))

###################

class TestSunspotServer(unittest.TestCase):
    ''' note that test methods must start with the word 'test' '''

    def setUp(self):

        self.constants = Constants(str(pathlib.Path(pathlib.Path(constants.__file__).parent, "constants_config.yaml")))
        


    def testGetFullDb(self):
        ''' tests requesting a copy of the full database from the server''' 

        # create a protobuf message to send to the server to request the database
        protoObj = SunspotMessages.ActualSunspotMessage()

        protoObj.timestamp = str(arrow.utcnow().timestamp)
        protoObj.message_type = SunspotMessages.ActualSunspotMessage.SERVER_QUERY

        protoObj.server_query_message.asking_for = SunspotMessages.ServerQuery.NEED_FULL_DB
        protoObj.server_query_message.requested_compression.append(SunspotMessages.COMPRESSION_NONE)
        protoObj.server_query_message.requested_compression.append(SunspotMessages.COMPRESSION_LZMA_XZ)

        data = protoObj.SerializeToString()
        print("The object is {}".format(data))

        content = None
        with closing(requests.post(self.constants.REMOTE_ENDPOINT, data=data, 
            headers={'Content-Type': 'application/octet-stream'}, stream=True)) as r:

            if r.status_code != 200:

                self.fail("Response status code was not 200, it was {}".format(r.status_code))

            print("reading content")
            content = r.content

        print("got content, it is this long: {}".format(len(content)))

        respObj = SunspotMessages.ActualSunspotMessage()
        respObj.ParseFromString(content)

        serverResponse = respObj.server_response_message
        print("version of db: {}".format(serverResponse.version_of_db))
        print("compression type: {}".format(serverResponse.compression_type))

            


# run the unit tests
if __name__ == '__main__':
    unittest.main()