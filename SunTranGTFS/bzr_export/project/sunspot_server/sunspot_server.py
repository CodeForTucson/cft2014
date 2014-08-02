
# TODO: why do i need these imports? find out later!
import sys
sys.stdout = sys.stderr

import hashlib
import atexit
import threading
import cherrypy

#####################################################

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

# TODO: HACK or else it won't import the pb2 class
sys.path.append("/var/www/sunspot")
cherrypy.log(str(sys.path))

from gtfs_data_pb2 import GTFSData



class Root:

    # TODO make this use pathlib?
    SERVER_WORKING_DIR = "/var/www/sunspot"
    GTFS_ZIP_FILE_STRING = os.path.join(SERVER_WORKING_DIR, "gtfs.zip")
    GTFS_ZIP_FILE_URL = "http://suntran.com/gtfs/SuntranGTFS.zip"
    HTTP_HEADER_LAST_MODIFIED_FORMAT = "ddd, D MMM YYYY HH:mm:ss"
    CONFIG_FILE_PATH = "/var/www/sunspot/config.yaml"

    def __init__(self):

        pass

    @cherrypy.expose
    def index(self):


        configObj = ServerConfig.load(Root.CONFIG_FILE_PATH)

        # if not os.path.exists(Root.GTFS_ZIP_FILE_STRING):

            
        # if we don't have the zip file yet, download it
        # make sure we use stream=True to not run into 'potential' memory problems
        


        with closing(requests.get(Root.GTFS_ZIP_FILE_URL, stream=True)) as resp:

            cherrypy.log("\tResponse was: {}".format(resp.status_code))

            if resp.status_code == 200:

                # see if the 'last modified' date has changed
                remoteDate = arrow.get(resp.headers["last-modified"], Root.HTTP_HEADER_LAST_MODIFIED_FORMAT)

                configDate = arrow.get(configObj["gtfs_zip_last_modified"])

                if remoteDate > configDate:

                    # set new modified date
                    configObj["gtfs_zip_last_modified"] = remoteDate.isoformat()

                    cherrypy.log("Remote last modified ({}) date is bigger then config date ({}), downloading new file".format(remoteDate, configDate))
                    cherrypy.log("Downloading file from {}".format(Root.GTFS_ZIP_FILE_URL))
                    cherrypy.log("Writing to file: {}".format(Root.GTFS_ZIP_FILE_STRING))
                    with open(Root.GTFS_ZIP_FILE_STRING, "wb") as f:
                        for iterFileChunk in resp.iter_content(chunk_size=10240):
                            f.write(iterFileChunk)
                        cherrypy.log("\tFile written successfully")

                else:
                    cherrypy.log("Remote last modified ({}) not bigger then config date ({}), not downloading".format(remoteDate, configDate))


        
        #cherrypy.log("gtfs file already downloaded")


        # now open the zip file and get the csv files from it


        gtfsZipFile = zipfile.ZipFile(Root.GTFS_ZIP_FILE_STRING, "r")
        cherrypy.log("Opened zip file successfully")

        finalData = GTFSData.FinalGtfsData()


        # we need certain files
        csvFilesDict = dict()
        
        # the io.TextIOWrapper is how we open a file within a ZipFile object in text mode rather then binary mode
        csvFilesDict["stops"] = io.TextIOWrapper(gtfsZipFile.open("stops.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["trips"] = io.TextIOWrapper(gtfsZipFile.open("trips.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["routes"] = io.TextIOWrapper(gtfsZipFile.open("routes.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["shapes"] = io.TextIOWrapper(gtfsZipFile.open("shapes.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["stop_times"] = io.TextIOWrapper(gtfsZipFile.open("stop_times.txt", "r"), encoding="utf-8", newline="")

        cherrypy.log("Opened txt files within zip files successfully")

        ####################
        # parse the stops.txt file
        ####################

        self.parseStopsTxt(finalData, csvFilesDict["stops"])


        ####################
        # parse the routes.txt file
        ####################

        self.parseRoutesTxt(finalData, csvFilesDict["routes"])

        # start with routes
        # routesReader = csv.DictReader(csvFilesDict["routes"])

        # routeOne = next(routesReader)
        # cherrypy.log("Choosing one route, it is {} - {}".format(routeOne["route_id"], routeOne["route_long_name"]))

        # now get a trip (first one has the same route id of the route we just got which
        # is good for testing since we are only doing once at the moment) from the csv

        # tripsReader = csv.DictReader(csvFilesDict["trips"])
        # tripOne = next(tripsReader)

        # cherrypy.log("Picking one trip, it is {} - {}".format(tripOne["trip_id"], tripOne["trip_short_name"]))

        # # now that we have a trip we use that trip_id to get all the stop_times (and later , stops)
        # # that correspond with that trip from the csv
        # listOfStopTimesDicts = list()
        # stopTimesReader = csv.DictReader(csvFilesDict["stop_times"])
        # for stopTimesRow in stopTimesReader:
        #     if stopTimesRow["trip_id"] == tripOne["trip_id"]:
        #         listOfStopTimesDicts.append(stopTimesRow)

        # cherrypy.log("There are {} stop times in this trip".format(len(listOfStopTimesDicts)))

        # # now collect all the stop ids
        # listOfStopIds = list()
        # for iterStopTimeDict in listOfStopTimesDicts:
        #     #cherrypy.log("stopSequence: {} -> stopId: {}".format(iterStopTimeDict["stop_sequence"], iterStopTimeDict["stop_id"]))
        #     listOfStopIds.append(iterStopTimeDict["stop_id"])


        # cherrypy.log("Got the stop ids we want, they are: {}".format(listOfStopIds))
        # # now collect all the relevant stops from the csv
        # # TODO: THIS IS THE STEP THAT IS GETTING THE STOP_SEQUENCES OUT OF ORDER
        # # its because we are iterating over the stops.txt csv rather then iterating over the listOfStopIds
        # # and then finding stuff inside the csv. (we are doing it the wrong way around)

        # listOfStopDicts = list()
        # stopReader = csv.DictReader(csvFilesDict["stops"])
        # for stopRow in stopReader:
        #     if stopRow["stop_id"] in listOfStopIds:
        #         listOfStopDicts.append(stopRow)



        # now we should have enough information to create a protocol buffer object

        cherrypy.log("Creating protobuf object")

        # # now seralize the protobuf message into a byte string
        byteString = finalData.SerializeToString() 

        cherrypy.log("Protobuf object created successfully, returning binary response")

        # set the mime type to be octet stream
        cherrypy.response.headers['Content-Type'] =  "application/octet-stream"


        return zlib.compress(byteString) 


    def parseRoutesTxt(self, protoBufObj, routesFileObj):
        ''' parse the routes.txt file object into the protobuf object
        '''

        routeReader = csv.DictReader(routesFileObj)

        for iterRow in routeReader:

            tmpProtoObj = protoBufObj.routes.add()

            tmpProtoObj.route_id = iterRow["route_id"]
            tmpProtoObj.agency_id = iterRow["agency_id"]
            tmpProtoObj.route_short_name = iterRow["route_short_name"]
            tmpProtoObj.route_long_name = iterRow["route_long_name"]
            tmpProtoObj.route_desc = iterRow["route_desc"]
            tmpProtoObj.route_type = int(iterRow["route_type"])
            tmpProtoObj.route_url = iterRow["route_url"]
            tmpProtoObj.route_color = iterRow["route_color"]
            tmpProtoObj.route_text_color = iterRow["route_text_color"]
            #tmpProtoObj.trips = 


    def parseStopsTxt(self, protoBufObj, stopsFileObj):
        ''' parse the stops.txt file object into the protobuf object
        '''

        stopReader = csv.DictReader(stopsFileObj)

        for iterRow in stopReader:


            tmpProtoObj = protoBufObj.stops.add()


            tmpProtoObj.stop_id = iterRow["stop_id"]
            tmpProtoObj.stop_code = iterRow["stop_code"]
            tmpProtoObj.stop_name = iterRow["stop_name"]
            tmpProtoObj.stop_desc = iterRow["stop_desc"]

            # create stop_coord
            tmpProtoObj.stop_coord.lat = float(iterRow["stop_lat"])
            tmpProtoObj.stop_coord.lon = float(iterRow["stop_lon"])

            tmpProtoObj.zone_id = iterRow["zone_id"]
            tmpProtoObj.stop_url = iterRow["stop_url"]
            if not iterRow["location_type"]: # in case its just blank
                tmpProtoObj.location_type = 0
            else:
                tmpProtoObj.location_type = iterRow["location_type"]
            tmpProtoObj.parent_station = iterRow["parent_station"]
            tmpProtoObj.wheelchair_boarding = int(iterRow["wheelchair_boarding"])




class ServerConfig:
    ''' class that just handles loading and saving our server config and whatnot
    '''

    def __init__(self):
        '''constructor'''

        self.theObj = None
        self.filePath = None

    @staticmethod
    def load(filePath):
        ''' loads and returns the yaml configuration file as a dict'''


        tmp = ServerConfig()
        tmp.theObj = yaml.safe_load(open(filePath, "rb"))
        tmp.filePath = filePath

        return tmp

    def save(self):
        ''' saves the config'''

        with open(self.filePath, "w", encoding="utf-8") as f:
            f.write(yaml.safe_dump(self.theObj))


    def __getitem__(self, key):
        '''implementation of obj[item]'''

        return self.theObj["sunspot_server_config"][key]

    def __setitem__(self, key, value):
        '''implementation of obj[item] = something'''

        self.theObj["sunspot_server_config"][key] = value

        self.save()


    def __delitem__(self, key):
        '''implementation of del obj["item"]'''

        del self.theObj[key]

        self.save()







config = {'/':
    {
        "log.error_file": "/var/www/sunspot_error.log"
    } }

application = cherrypy.Application(Root(), script_name=None, config=config)