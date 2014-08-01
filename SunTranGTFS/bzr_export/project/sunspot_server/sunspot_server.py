
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
import requests
import zipfile
import csv
import io

# TODO: HACK or else it won't import the pb2 class
sys.path.append("/var/www/sunspot")
cherrypy.log(str(sys.path))

from gtfs_data_pb2 import GTFSData



class Root:

    # TODO make this use pathlib?
    SERVER_WORKING_DIR = "/var/www/sunspot"
    GTFS_ZIP_FILE_STRING = os.path.join(SERVER_WORKING_DIR, "gtfs.zip")
    GTFS_ZIP_FILE_URL = "http://suntran.com/gtfs/SuntranGTFS.zip"

    def __init__(self):

        pass

    @cherrypy.expose
    def index(self):

        if not os.path.exists(Root.GTFS_ZIP_FILE_STRING):

            
            # if we don't have the zip file yet, download it
            # make sure we use stream=True to not run into 'potential' memory problems
            cherrypy.log("Downloading file from {}".format(Root.GTFS_ZIP_FILE_URL))
            resp = requests.get(Root.GTFS_ZIP_FILE_URL, stream=True)
            cherrypy.log("\tResponse was: {}".format(resp.status_code))

            if resp.status_code == 200:
                cherrypy.log("Writing to file: {}".format(Root.GTFS_ZIP_FILE_STRING))
                with open(Root.GTFS_ZIP_FILE_STRING, "wb") as f:
                    for iterFileChunk in resp.iter_content(chunk_size=10240):
                        f.write(iterFileChunk)
                    cherrypy.log("\tFile written successfully")


        
        cherrypy.log("gtfs file already downloaded")


        # now open the zip file and get the csv files from it


        gtfsZipFile = zipfile.ZipFile(Root.GTFS_ZIP_FILE_STRING, "r")
        cherrypy.log("Opened zip file successfully")

        # we need certain files
        csvFilesDict = dict()
        
        csvFilesDict["stops"] = io.TextIOWrapper(gtfsZipFile.open("stops.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["trips"] = io.TextIOWrapper(gtfsZipFile.open("trips.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["routes"] = io.TextIOWrapper(gtfsZipFile.open("routes.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["shapes"] = io.TextIOWrapper(gtfsZipFile.open("shapes.txt", "r"), encoding="utf-8", newline="")
        csvFilesDict["stop_times"] = io.TextIOWrapper(gtfsZipFile.open("stop_times.txt", "r"), encoding="utf-8", newline="")

        cherrypy.log("Opened txt files within zip files successfully")

        # start with routes
        routesReader = csv.DictReader(csvFilesDict["routes"])

        routeOne = next(routesReader)
        cherrypy.log("Choosing one route, it is {} - {}".format(routeOne["route_id"], routeOne["route_long_name"]))

        # now get a trip (first one has the same route id of the route we just got which
        # is good for testing since we are only doing once at the moment) from the csv

        tripsReader = csv.DictReader(csvFilesDict["trips"])
        tripOne = next(tripsReader)

        cherrypy.log("Picking one trip, it is {} - {}".format(tripOne["trip_id"], tripOne["trip_short_name"]))

        # now that we have a trip we use that trip_id to get all the stop_times (and later , stops)
        # that correspond with that trip from the csv
        listOfStopTimesDicts = list()
        stopTimesReader = csv.DictReader(csvFilesDict["stop_times"])
        for stopTimesRow in stopTimesReader:
            if stopTimesRow["trip_id"] == tripOne["trip_id"]:
                listOfStopTimesDicts.append(stopTimesRow)

        cherrypy.log("There are {} stop times in this trip".format(len(listOfStopTimesDicts)))

        # now collect all the stop ids
        listOfStopIds = list()
        for iterStopTimeDict in listOfStopTimesDicts:
            #cherrypy.log("stopSequence: {} -> stopId: {}".format(iterStopTimeDict["stop_sequence"], iterStopTimeDict["stop_id"]))
            listOfStopIds.append(iterStopTimeDict["stop_id"])


        cherrypy.log("Got the stop ids we want, they are: {}".format(listOfStopIds))
        # now collect all the relevant stops from the csv
        # TODO: THIS IS THE STEP THAT IS GETTING THE STOP_SEQUENCES OUT OF ORDER
        # its because we are iterating over the stops.txt csv rather then iterating over the listOfStopIds
        # and then finding stuff inside the csv. (we are doing it the wrong way around)

        listOfStopDicts = list()
        stopReader = csv.DictReader(csvFilesDict["stops"])
        for stopRow in stopReader:
            if stopRow["stop_id"] in listOfStopIds:
                listOfStopDicts.append(stopRow)

                #cherrypy.log("stopId: {}".format(stopRow["stop_id"]))


        # now we should have enough information to create a protocol buffer object

        cherrypy.log("Creating protobuf object")
        protoBufObj = GTFSData.TempListOfStops()
        for iterStopDict in listOfStopDicts:

            tmpProtoObj = protoBufObj.stops.add()

            tmpProtoObj.stop_id = iterStopDict["stop_id"]
            tmpProtoObj.stop_code = iterStopDict["stop_code"]
            tmpProtoObj.stop_name = iterStopDict["stop_name"]
            tmpProtoObj.stop_desc = iterStopDict["stop_desc"]
            tmpProtoObj.stop_lat = iterStopDict["stop_lat"]
            tmpProtoObj.stop_lon = iterStopDict["stop_lon"]
            tmpProtoObj.zone_id = iterStopDict["zone_id"]
            tmpProtoObj.stop_url = iterStopDict["stop_url"]
            if not iterStopDict["location_type"]: # in case its just blank
                tmpProtoObj.location_type = 0
            else:
                tmpProtoObj.location_type = iterStopDict["location_type"]
            tmpProtoObj.parent_station = iterStopDict["parent_station"]
            tmpProtoObj.wheelchair_boarding = int(iterStopDict["wheelchair_boarding"])

        # now seralize the protobuf message into a byte string
        byteString = protoBufObj.SerializeToString() 

        cherrypy.log("Protobuf object created successfully, returning binary response")

        # set the mime type to be octet stream
        cherrypy.response.headers['Content-Type'] =  "application/octet-stream"


        return byteString 



config = {'/':
    {
        "log.error_file": "/var/www/sunspot_error.log"
    } }

application = cherrypy.Application(Root(), script_name=None, config=config)