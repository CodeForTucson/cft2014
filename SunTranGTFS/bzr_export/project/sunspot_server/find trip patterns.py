

import csv
import zipfile
import io
import pprint

# find trip patterns


# you can replace this ZipFile object with an already open ZipFile or ZipFileEx

gtfsZipFile = zipfile.ZipFile("/Users/markgrandi/Code/bzr_new/winphone_tucson_suntran_app_repo/trunk/example files/SuntranGTFS.zip", "r")


routesFile = io.TextIOWrapper(gtfsZipFile.open("routes.txt", "r"), encoding="utf-8", newline="")
tripsFile = io.TextIOWrapper(gtfsZipFile.open("trips.txt", "r"), encoding="utf-8", newline="")

# stopTimesFileData = gtfsZipFile.open("stop_times.txt", "r")
stopTimeFileData = io.StringIO(io.TextIOWrapper(gtfsZipFile.open("stop_times.txt", "r"), encoding="utf-8", newline="").read())


# for every route
for iterRouteDict in csv.DictReader(routesFile):


    iterRouteId = iterRouteDict["route_id"]
    print("doing route id {}".format(iterRouteId))

    listOfTripIds = list()

    # collect the trip ids for this route
    # TODO this won't work once we keep looping in the outer for loop, can we seek to 0? or do we have
    # to reopen it...
    for iterTripDict in csv.DictReader(tripsFile):


        if iterTripDict["route_id"] == iterRouteId:

            listOfTripIds.append(iterTripDict["trip_id"])



    # now for each trip id, we need to create a "list" of the stop ids (in order) from stop_times.txt and then store it
    # and then for each trip see if we have stored this 'list of stop ids' before , to actually find the patterns

    listOfStopIdPatterns = list()

    print("Have {} trip ids to look at".format(len(listOfTripIds)))
    counter = 1

    for iterTripId in listOfTripIds:

        print("{}/{} - tripId: {}".format(counter, len(listOfTripIds), iterTripId))

        tmpPattern = list()

        # have to create this every time =/ 
        # stopTimesFileData.seek(0)
        

        stopTimeFileData.seek(0)
        for iterStopTimeRow in csv.DictReader(stopTimeFileData):

            if iterStopTimeRow["trip_id"] == iterTripId:
                tmpPattern.append(iterStopTimeRow["stop_id"])


        # see if this pattern has been seen before
        if tmpPattern not in listOfStopIdPatterns:
            print("Found new pattern: {}".format(tmpPattern))
            listOfStopIdPatterns.append(tmpPattern)

        counter += 1

    print("Stop patterns that we found: \n{}".format(pprint.pformat(listOfStopIdPatterns)))

    break