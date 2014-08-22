#!/usr/bin/env python3
#
#
# script to download the gtfs data if needed, generate the sqlite3 database
# from that information, and then generate bsdiff4 patches from every previous
# database to the current version of the database
#
from constants import Constants
from server_config import ServerConfig, ConfigEnums

import logging
import collections
import sqlite3
import argparse
import sys
import os.path
import contextlib
import zipfile
import io
import csv
import shutil
import pathlib

# third party
import requests
import arrow
import bsdiff4


StopTimesGroup = collections.namedtuple("StopTimesGroup", ["stopTimeList"])
IndividualStopTime = collections.namedtuple("IndividualStopTime", ["arrival_time", "departure_time", "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled"])


DownloadResult = collections.namedtuple("DownloadResult", ["zipFilePath", "databaseTime"])

IsSqliteResult = collections.namedtuple("IsSqliteResult", ["sqlitePath", "dbConn"])

class GTFSDataParser:

    def __init__(self):
        pass


    def parse(self, args, logger):

        logger = logger.getChild("parser")
        self.constants = Constants(args.constantsYamlPath)
        
        configObj = ServerConfig(self.constants.CONFIG_DB_PATH, logger)


        if args.verbose:
            logger.setLevel(logging.DEBUG)

        logger.debug("Program arguments are: {}".format(args))

        ###################
        # DOWNLOADING
        ###################
            
        # used to see if we got a new database or not
        try:
            oldDownloadTime = configObj[ConfigEnums.KEY_LAST_DOWNLOAD_TIME]
        except KeyError:
            # don't have a oldDownloadTime set, so therefore its a brand new database
            oldDownloadTime = "0"

        # figure out what GTFS data we are using, and if it needs to be downloaded
        downloadResult = self.downloadGtfsDataIfNeeded(configObj, logger, args)



        logger.debug("downloadResult is: {}".format(downloadResult))
            
        databaseTime = downloadResult.databaseTime

        shouldGeneratePatches = oldDownloadTime != str(databaseTime)


        logger.info("Are we skipping database generation? {}".format(downloadResult.zipFilePath is None))
        logger.info("Did we get a new database and generating new patches? {}".format(shouldGeneratePatches))
        
        ###################
        # FINDING DATABASE PATH
        ###################

        # Figure out where we are creating the database

        logger.debug("database time is: {}".format(databaseTime))

        dbMaybePath = None
        if not args.forceuse:
            dbMaybePath = pathlib.Path(self.constants.DB_FOLDER, "{}.sqlite3".format(databaseTime))

        else:
            dbMaybePath = pathlib.Path(pathlib.Path(args.forceuse).parent, "db_forceuse_{}.sqlite".format(arrow.utcnow().timestamp))

        logger.debug("dbMaybePath is: {}".format(dbMaybePath))

        if not dbMaybePath.parent.exists():
            logger.debug("Creating parent directory for {}".format(dbMaybePath))
            dbMaybePath.parent.mkdir()

        # Open the database connection
        sqliteResult = None

        # if downloadResult.zipFilePath is None, then we already have the database generated no need to generate it again
        if downloadResult.zipFilePath is  None:

            # not creating the database
            # pass force=True cause we know the database exists
            try:
                sqliteResult = isSqliteValidLocationType(str(dbMaybePath), True)
            except Exception:
                logger.exception("Unable to create sqlite database at {}".format(dbMaybePath))
                sys.exit(1)

        else:
            # we are creating the database

            ###################
            # CREATING DATABASE
            ###################

            try:
                sqliteResult = isSqliteValidLocationType(str(dbMaybePath))
            except Exception:
                logger.exception("Unable to create sqlite database at {}".format(dbMaybePath))
                sys.exit(1)

            logger.info("Saving database to {}".format(sqliteResult.sqlitePath))

            db = sqliteResult.dbConn
            cursor = db.cursor()

            # now open the zip file and get the csv files from it
            try:
                gtfsZipFile = zipfile.ZipFile(downloadResult.zipFilePath, "r")
            except Exception as e:
                logger.exception("Unable to create ZipFile object at {}".format(downloadResult.zipFilePath))

            logger.debug("Opened zip file successfully")
            csvFilesDict = dict()
            # the io.TextIOWrapper is how we open a file within a ZipFile object in text mode rather then binary mode
            csvFilesDict["stops"] = io.TextIOWrapper(gtfsZipFile.open("stops.txt", "r"), encoding="utf-8", newline="")
            csvFilesDict["trips"] = io.TextIOWrapper(gtfsZipFile.open("trips.txt", "r"), encoding="utf-8", newline="")
            csvFilesDict["routes"] = io.TextIOWrapper(gtfsZipFile.open("routes.txt", "r"), encoding="utf-8", newline="")
            csvFilesDict["shapes"] = io.TextIOWrapper(gtfsZipFile.open("shapes.txt", "r"), encoding="utf-8", newline="")
            csvFilesDict["stop_times"] = io.TextIOWrapper(gtfsZipFile.open("stop_times.txt", "r"), encoding="utf-8", newline="")

            logger.debug("Opened txt files within zip files successfully")

            ####################
            # parse the stops.txt file
            ####################

            self.parseStopsTxt(cursor, csvFilesDict["stops"], logger)
            db.commit()

            ####################
            # parse the routes.txt file
            ####################

            self.parseRoutesTxt(cursor, csvFilesDict["routes"], logger)
            db.commit()


            ###################
            # parse trips.txt 
            ###################

            self.parseTrips(cursor, csvFilesDict["trips"], logger)
            db.commit()

            ###################
            # parse stop_times.txt
            ###################

            self.parseStopTimes(cursor, csvFilesDict["stop_times"], logger)
            db.commit()

            ###################
            # parse shapes.txt
            ###################

            self.parseShapes(cursor, csvFilesDict["shapes"], logger)
            db.commit()

            ###################
            # find trip patterns for each route
            ###################

            self.findTripPatterns(db, logger)
            db.commit()

            # close the database
            db.close()

            # we are done if we are forceusing a zip file
            if  args.forceuse:
                logger.info("Done")
                return



            logger.info("Chowning")

            # chown the database to be whatever our constant is so the server can access it
            shutil.chown(sqliteResult.sqlitePath, self.constants.CHOWN_OWNER, self.constants.CHOWN_GROUP)

            logger.info("Inserting database path into config database")

            # insert the database's location into our config database
            configObj.setWithPrefix(ConfigEnums.PREFIX_DATABASE_TIME_TO_LOCATION, [databaseTime], sqliteResult.sqlitePath)

            # then , create patches for every database version up to this one

        if shouldGeneratePatches:
            self.createDbPatches(databaseTime, sqliteResult, configObj, logger)
        else:
            logger.info("skipping patch generation")

        logger.info("done")
        


    def createDbPatches(self, generatedDbTime, sqliteResultObj, serverConfig, logger):
        ''' this creates a patch file from every sqlite database we have generated (according to our ServerConfig database),
        to the current one we just generated in this script (identified by generatedDbTime)

        @param generatedDbTime - the time / version of the database we just generated
        @param sqliteResultObj - the SqliteResult object , we need the path for the database
        @param serverConfig - the ServerConfig object
        @param logger - the logger'''


        lg = logger.getChild("patching")

        # we start out by deleting everything in the db patches folder, and clearing any ServerConfig keys
        # that link to other patches. We do this because when this method is called, we are getting a new database
        # and therefore we need to generate new patches anyway
        for iterPatchFile in pathlib.Path(self.constants.DB_PATCHES_FOLDER).glob("*.bsdiff4"):
            lg.debug("Deleting old patch file: {}".format(iterPatchFile))
            iterPatchFile.unlink()

        lg.debug("Deleting old patch entries in server config db")
        serverConfig.deleteAllInRange(ConfigEnums.KEY_DB_PATCH_LETTER, [])


        lg.info("Starting database patch creation, current database time is {}".format(generatedDbTime))
        for iterKey, iterValue in serverConfig.getGeneratorWithPrefix(ConfigEnums.PREFIX_DATABASE_TIME_TO_LOCATION, [""]):

            tmpKeyStr = iterKey.decode("utf-8")
            prevDbVersion = tmpKeyStr[tmpKeyStr.find(":") + 1:]
            prevDbPath = iterValue.decode("utf-8")
            lg.debug(" On key: '{}', value: '{}'".format(prevDbVersion, prevDbPath))



            if prevDbVersion == str(generatedDbTime):

                lg.debug("  Key is the same as the generatedDbTime, no need to patch to ourselves, skipping")
                continue


            # see if we even need to generate a patch
            existingEntry = None
            keepGoing = False
            try:
                existingEntry = serverConfig.getWithPrefix(ConfigEnums.PREFIX_DB_PATCH, [prevDbVersion, generatedDbTime])
            except KeyError:
                # doesn't exist, keep going
                keepGoing = True

            if not keepGoing:
                # we have a database entry already, does the patch exist on filesystem?
                existingPatchPath = pathlib.Path(existingEntry)
                if existingPatchPath.exists():
                    # don't need to generate a patch
                    lg.info("  patch already exists for database version {} to {} at {}, skipping"
                        .format(prevDbVersion, generatedDbTime, existingPatchPath))
                    continue
                else:
                    # shouldn't happen, but generate a patch as we don't have one on disk
                    lg.debug("  Entry exists in database but no patch found on disk, generating patch")


            lg.debug("  creating patch from database version {} to current version {}".format(prevDbVersion, generatedDbTime))

            # get the path to the current database
            curDbPath = serverConfig.getWithPrefix(ConfigEnums.PREFIX_DATABASE_TIME_TO_LOCATION, [generatedDbTime])

            # where are we storing the patch?
            patchDir = pathlib.Path(self.constants.DB_PATCHES_FOLDER)
            if not patchDir.exists():
                patchDir.mkdir()
            patchPath = pathlib.Path(patchDir, "{}_to_{}_patch.bsdiff4".format(prevDbVersion, generatedDbTime))

            # generate the patch
            lg.debug("   src_path: {}".format(prevDbPath))
            lg.debug("   dst_path: {}".format(curDbPath))
            lg.debug("   Saving patch to {}".format(patchPath))
            # src_path, dst_path, patch_path
            bsdiff4.file_diff(prevDbPath, curDbPath, str(patchPath))

            # then save the fact that we have a patch into our config database
            serverConfig.setWithPrefix(ConfigEnums.PREFIX_DB_PATCH, [prevDbVersion, generatedDbTime], str(patchPath))

            





    def downloadGtfsDataIfNeeded(self, configObj, loggerObj, namespaceObj):
        ''' the logic to see if we need to download the gtfs zip data, 
        and returns a DownloadResult object with the path of the zip file and the 
        time of the database we are considering

        @param logger - a logger object
        @param namespaceObj - the argparse namespace object
        @return a DownloadResult object'''


        logger = loggerObj.getChild("download")

        # see if we even need to download the zip file or if we are manually using one
        if namespaceObj.forceuse:
            return DownloadResult(namespaceObj.forceuse, 0)

        # if we don't have the zip file yet, download it
        # make sure we use stream=True to not run into 'potential' memory problems
        databaseTime = None

        logger.debug("making request to {}".format(self.constants.GTFS_ZIP_FILE_URL))
        with contextlib.closing(requests.get(self.constants.GTFS_ZIP_FILE_URL, stream=True)) as resp:

            logger.debug("Response was: {}".format(resp.status_code))

            if resp.status_code == 200:

                # see if the 'last modified' date has changed
                remoteDate = arrow.get(resp.headers["last-modified"], self.constants.HTTP_HEADER_LAST_MODIFIED_FORMAT)

                # in case we don't have a config date set
                configDate = None
                try:
                    configDate = arrow.get(configObj[ConfigEnums.KEY_LAST_DOWNLOAD_TIME])
                except KeyError:
                    logger.debug("KeyError, setting configDate to be epoch")
                    configDate = arrow.get("1970-01-01T00:00:00+00:00")


                conditionOne = remoteDate > configDate
                conditionTwo = not os.path.exists(self.constants.GTFS_ZIP_FILE_STRING)
                if conditionOne or conditionTwo:

                    # need to download new GTFS data
                    if conditionTwo:
                        logger.debug("file doesn't exist at {}, downloading".format(self.constants.GTFS_ZIP_FILE_STRING))
                    if conditionOne:
                        logger.debug("Remote last modified ({}) date is bigger then config date ({}), or downloading new file".format(remoteDate, configDate))
                    
                    logger.debug("Downloading file from {}".format(self.constants.GTFS_ZIP_FILE_URL))
                    logger.debug("Writing to file: {}".format(self.constants.GTFS_ZIP_FILE_STRING))
                    with open(self.constants.GTFS_ZIP_FILE_STRING, "wb") as f:
                        for iterFileChunk in resp.iter_content(chunk_size=10240):
                            f.write(iterFileChunk)
                        logger.debug("\tFile written successfully")

                    # set new modified date after we finished downloading
                    configObj[ConfigEnums.KEY_LAST_DOWNLOAD_TIME] = str(remoteDate.timestamp)
                    databaseTime = remoteDate.timestamp

                    return DownloadResult(self.constants.GTFS_ZIP_FILE_STRING, databaseTime)

                else:
                    # don't need to download new GTFS data

                    logger.info("Remote last modified ({}) not bigger then config date ({}), not downloading".format(remoteDate, configDate))
                    databaseTime = configDate.timestamp

                    # see if the database exists for this time, aka if we need to recreate the database using the existing gtfs data
                    try:
                        tmppath = configObj.getWithPrefix(ConfigEnums.PREFIX_DATABASE_TIME_TO_LOCATION, [configDate.timestamp])
                        if pathlib.Path(tmppath).exists():

                            # database already exists so we can skip generation, by passing None in for DownloadResult.zipFilePath
                            logger.info("Database at location {} already exists".format(tmppath))
                            return DownloadResult(None, configDate.timestamp)
                        else:
                            logger.info("Database doesn't exist at location {}, creating".format(tmppath))
                            return DownloadResult(self.constants.GTFS_ZIP_FILE_STRING, configDate.timestamp)
                    except KeyError:
                        # program ended prematurely? this gets set at the end of the script so if it doesn't exist 
                        # then it obviously doesn't exist
                        logger.info("Database doesn't exist yet for this date, creating")
                        return DownloadResult(self.constants.GTFS_ZIP_FILE_STRING, configDate.timestamp)

            else:
                logger.error("Didn't get status code of 200, got {}".format(resp.status_code))
                sys.exit(1)      

    def parseRoutesTxt(self, cursor, routesFileObj, logger):
        ''' parse the routes.txt file object into the sqlite3 database
        '''


        rLogger = logger.getChild("routes")
        routeReader = csv.reader(routesFileObj)
        next(routeReader) # skip first line of field names

        # create the table
        cursor.execute('''CREATE TABLE routes
            (route_id TEXT, agency_id TEXT, route_short_name TEXT, 
            route_long_name TEXT, route_desc TEXT, route_type INTEGER, 
            route_url TEXT, route_color TEXT, route_text_color TEXT)''')

        # create the index
        cursor.execute('''CREATE INDEX routes_routeid_index ON routes (route_id)''')

        rLogger.debug("routes table/index created successfully")

        # insert data
        counter = 0
        for iterRow in routeReader:

            cursor.execute('''INSERT INTO routes VALUES
                (?,?,?,?,?,?,?,?,?)''', tuple(iterRow))
            counter += 1

        rLogger.debug("inserted {} rows into routes table".format(counter))


    def parseStopsTxt(self, cursor, stopsFileObj, logger):
        ''' parse the stops.txt file object into sqlite3 database
        '''

        stopReader = csv.reader(stopsFileObj)
        next(stopReader) # skip first line of field names
        sLogger = logger.getChild("stops")

        # create the table
        # stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,wheelchair_boarding
        cursor.execute('''CREATE TABLE stops
            (stop_id TEXT, stop_code TEXT, stop_name TEXT, stop_desc TEXT,
            stop_lat REAL, stop_lon REAL, zone_id TEXT, stop_url TEXT, location_type INTEGER,
            parent_station TEXT, wheelchair_boarding TEXT)''')

        # create the index
        cursor.execute('''CREATE INDEX stops_stopid_index ON stops (stop_id)''')

        sLogger.debug("stops table/index created successfully")

        # insert data
        counter = 0
        for iterRow in stopReader:
            cursor.execute('''INSERT INTO stops VALUES
                (?,?,?,?,?,?,?,?,?,?,?)''', tuple(iterRow))
            counter += 1

        sLogger.debug("inserted {} rows into stops table".format(counter))


    def parseTrips(self, cursor, tripsFileObj, logger):
        ''' parse trips.txt file object into the sqlite3 database '''

        tripReader = csv.reader(tripsFileObj)
        next(tripReader) # skip first line of field names
        tLogger = logger.getChild("trips")

        # create table
        # route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id
        cursor.execute('''CREATE TABLE trips
            (route_id TEXT, service_id TEXT, trip_id TEXT, trip_headsign TEXT,
            trip_short_name TEXT, direction_id INTEGER, block_id TEXT, shape_id TEXT)''')

        # create the index
        cursor.execute('''CREATE INDEX trips_routeid_index ON trips (route_id)''')
        cursor.execute('''CREATE INDEX trips_tripid_index ON trips (trip_id)''')

        tLogger.debug("trips table/index created successfully")

        # insert data
        counter = 0
        for iterRow in tripReader:

            cursor.execute('''INSERT INTO trips VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)''', tuple(iterRow))
            counter += 1

        tLogger.debug("inserted {} rows into trips table".format(counter))


    def parseStopTimes(self, cursor, stopTimesFileObj, logger):
        '''parse stop_times.txt file object into the sqlite3 database'''

        sTimesReader = csv.reader(stopTimesFileObj)
        next(sTimesReader) # skip first line of field names
        stLogger = logger.getChild("stop_times")

        # create table
        # trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled
        cursor.execute('''CREATE TABLE stop_times
            (trip_id TEXT, arrival_time TEXT, departure_time TEXT, stop_id TEXT, stop_sequence INTEGER, 
            stop_headsign TEXT, pickup_type INTEGER, drop_off_type INTEGER, shape_dist_traveled REAL)''')

        # create indexes
        cursor.execute('''CREATE INDEX stoptimes_tripid_index ON stop_times (trip_id)''')
        cursor.execute('''CREATE INDEX stoptimes_stopid_index ON stop_times (stop_id)''')

        stLogger.debug("stop_times table/index created successfully")

        # insert data
        counter = 0
        for iterRow in sTimesReader:

            cursor.execute('''INSERT INTO stop_times VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)''', tuple(iterRow))
            counter +=1

        stLogger.debug("inserted {} rows into stop_times table".format(counter))


    def parseShapes(self, cursor, shapesFileObj, logger):
        ''' parse shapes.txt file object into the sqlite3 database'''

        shapeReader = csv.reader(shapesFileObj)
        next(shapeReader) # skip first line of field names
        sLogger = logger.getChild("shapes")

        # create table
        # shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled
        cursor.execute('''CREATE TABLE shapes
            (shape_id TEXT, shape_pt_lat REAL, shape_pt_lon REAL, shape_pt_sequence INTEGER,
            shape_dist_traveled REAL)''')

        # create index
        cursor.execute('''CREATE INDEX shape_shapeid_index ON shapes (shape_id)''')

        sLogger.debug("shapes table/index created successfully")

        # insert data
        counter = 0
        for iterRow in shapeReader:

            cursor.execute('''INSERT INTO shapes VALUES
                (?, ?, ?, ?, ?)''', tuple(iterRow))
            counter += 1

        sLogger.debug("inserted {} rows into the shapes table".format(counter))


    def findTripPatterns(self, db, logger):
        ''' use the data that we put into the database to find the trip patterns
        for each route, and then also put that into the database

        basically,

        for each routes
            find the trip ids for that route
                for each trip id, find the stop times (in order) for that trip id
                    save that into a list, and then for additional patterns see if it exists first
        '''

        # we need multiple cursors, maybe i could do it with just one statement but i can't think an easy way atm

        tpLogger = logger.getChild("trip_patterns")

        routeCursor = db.cursor()
        tripCursor = db.cursor()
        stopTimeCursor = db.cursor()
        tripPatternCursor = db.cursor()
        counter = 0

        # create the trip_patterns table
        tripPatternCursor.execute('''CREATE TABLE trip_patterns
            (route_id TEXT, trip_id TEXT)''')

        # create the index
        tripPatternCursor.execute('''CREATE INDEX trip_patterns_routeid_index ON trip_patterns (route_id)''')

        tpLogger.debug("trip_patterns table/index created successfully")

        routeCursor.execute('''SELECT route_id FROM routes''')

        for iterRouteRow in routeCursor.fetchall(): # go through every route
            
            #tpLogger.debug("on route {}".format(iterRouteRow["route_id"]))
            setOfTripPatternObjs = set() # the trip patterns for this route

            tripCursor.execute('''SELECT trip_id, trip_headsign, direction_id FROM trips WHERE route_id = ?''', (iterRouteRow["route_id"],))

            for iterTripRow in tripCursor.fetchall(): # go through every trip that is part of this route


                # get all the stops for this trip in order, and then add them to a temporary list, and then 
                # create a TripPattern object
                stopTimeCursor.execute('''SELECT stop_id FROM stop_times WHERE trip_id = ?
                    ORDER BY stop_sequence ASC''', (iterTripRow["trip_id"],))

                tmpPatternList = list()
                for iterStopTimeRow in stopTimeCursor.fetchall():
                    tmpPatternList.append(iterStopTimeRow["stop_id"])

                # create the TripPattern object and try to add it to the set
                # we also add a trip headsign (for debugging), direction id (for debugging), and the trip id
                # because then we can just look up in the stop_times table using the trip_id and then get the
                # order of the stops
                setOfTripPatternObjs.add(TripPattern(tuple(tmpPatternList), iterTripRow["trip_headsign"], 
                    iterTripRow["direction_id"], iterTripRow["trip_id"]))


            # now add this trip pattern information to 
            # tpLogger.debug("\ttrip patterns: {}".format(setOfTripPatternObjs))
            for iterTripPattern in setOfTripPatternObjs:
                tripPatternCursor.execute('''INSERT INTO trip_patterns VALUES (?, ?)''', (iterRouteRow["route_id"], iterTripPattern.exampleTripId))

                counter += 1

        tpLogger.debug("inserted {} rows into the trip_patterns table".format(counter))







class TripPattern:
    ''' this is an object that takes a 'pattern' (a tuple) of stop_ids that represent a trip,
    and uses this to compare to other TripPattern objects. This object also stores time values (a tuple)
    that signify what times this trip happens, but these times do NOT have an effect on hash/equality!'''

    def __init__(self, tupleOfValues, description, directionId, exampleTripId):

        self.tupleOfValues = tupleOfValues
        self.description = description
        self.direction = directionId
        self.exampleTripId = exampleTripId

    def __hash__(self):

        return hash(self.tupleOfValues)

    def __eq__(self, other):
        return self.tupleOfValues == other.tupleOfValues

    def __str__(self):
        return "<TripPattern: {} direction: {} , tripIdExample: {} >".format(self.description, self.direction, self.exampleTripId)

    def __repr__(self):
        return str(self)

def isSqliteValidLocationType(stringArg, force=False):
    ''' helper method for argparse which checks to see if sqlite3 can create a 
    database file at the specified location and throws an exception that argparse catches
    if it can't

    @param stringArg - the argument given to us by argparse
    @param force - whether to overwrite if the file exists 
    @return a TUPLE, first is the stringArg, second is the sqlite3 connection object or we throw an exception
    '''

    tmpPath = pathlib.Path(stringArg)
    if tmpPath.exists():
        if not force:
            raise argparse.ArgumentTypeError("the file already exists, not overwriting")

    try:

        tmpConnection = sqlite3.connect(os.path.realpath(stringArg))
        tmpConnection.row_factory = sqlite3.Row
        

    except sqlite3.OperationalError as e:
        raise argparse.ArgumentTypeError("Unable to open sqlite3 database at specified location: '{}', error: {}".format(os.path.realpath(stringArg), e))

    return IsSqliteResult(stringArg,tmpConnection)


if __name__ == "__main__":
    # if we are being run as a real program

    parser = argparse.ArgumentParser(description="parses the GTFS data for suntran", 
    epilog="Copyright Aug 6, 2014 Mark Grandi")
    
    #parser.add_argument('sqliteDbConn', metavar="sqliteOutput", type=isSqliteValidLocationType, help="Where to create (including filename) the sqlite3 database")
    parser.add_argument("constantsYamlPath", help="the path yaml that holds the constants")
    parser.add_argument("--verbose", "-v", action="store_true", help="increase verbosity")
    parser.add_argument("--forceuse", help="specify a filepath to use the specified zip file rather then downloading it")


    rootLogger = logging.getLogger("GTFSDataParser")
    logging.basicConfig(level="INFO", format='%(asctime)s %(name)-35s %(levelname)-8s: %(message)s')

    rootLogger.info("Starting")
    try:
        gtfsParser = GTFSDataParser()

        gtfsParser.parse(parser.parse_args(), rootLogger)
    except Exception as e:

        rootLogger.critical("Something went wrong...")
        rootLogger.exception(e)