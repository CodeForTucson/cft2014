#!/usr/bin/env python3
#
# script to test the patching to make sure that after we patch we get the file hash we expect
# 
# Mark Grandi - Aug 22, 2014
#

import argparse, sys, logging, pathlib, hashlib, pprint

import leveldb
import bsdiff4
import arrow

from server_database import ServerDatabase, ServerDatabaseEnums

def testPatching(args):
    '''tests the db patches generating the expected databases by using the information in the provided ServerDatabase leveldb database
    @param args - the namespace object we get from argparse.parse_args()
    '''

    lg = logging.getLogger("main")

    if args.verbose:
        lg.setLevel("DEBUG")


    # open the ServerDatabase
    sbObj = ServerDatabase(args.serverDatabasePath, lg)

    latestDbHasher = hashlib.sha256()

    # first find the most recent database, this is what all the other databases + the patch should equal to.
    latestDbTime = sbObj[ServerDatabaseEnums.KEY_LAST_DOWNLOAD_TIME]
    latestDbResult = sbObj.getWithPrefix(ServerDatabaseEnums.PREFIX_DATABASE_TIME_TO_LOCATION, [latestDbTime])
    latestDbFilepath = pathlib.Path(latestDbResult)

    lg.info("Latest db time is: {}".format(latestDbTime))
    lg.info("Latest db is located at: {}".format(latestDbFilepath))

    with open(str(latestDbFilepath), "rb") as f:
        latestDbHasher.update(f.read())
    latestDbHexDigest = latestDbHasher.hexdigest()

    lg.info("Latest db's sha256 is {}".format(latestDbHexDigest))

    # now go through every database we can find, and then get its associated patch, patch the database and see if it 
    # equals the latest database's hash

    errorList = list()
    for iterDbKey, iterDbValue in sbObj.getGeneratorWithPrefix(ServerDatabaseEnums.PREFIX_DATABASE_TIME_TO_LOCATION, [""]):


        iterDbTime = iterDbKey.decode("utf-8")
        iterDbTime = iterDbTime[iterDbTime.find(":")+1:]
        iterDbPath = iterDbValue.decode("utf-8")

        lg.info("on database version: {} ({})".format(iterDbTime, arrow.get(iterDbTime).isoformat()))


        if iterDbTime == latestDbTime:
            lg.info("\tSkipping database because its the same as the latest db")
            continue

        # get the patch between this database and the latest database
        try:
            iterPatchResult = sbObj.getWithPrefix(ServerDatabaseEnums.PREFIX_DB_PATCH, [iterDbTime, latestDbTime])
        except KeyError:
            lg.error("\tPatch entry from {} to {} doesn't exist in the ServerDatabase!".format(iterDbTime, latestDbTime))
            errorList.append(iterDbTime)
            continue

        iterPatchPath = pathlib.Path(iterPatchResult)

        if not iterPatchPath.exists():
            lg.info("\tERROR: patch from {} to {} doesn't exist on disk ({})".format(iterDbTime, iterDbPath, iterPatchPath))
            errorList.append(iterDbTime)
            continue

        # apply the patch

        iterDbFile = open(str(iterDbPath), "rb")
        iterPatchFile = open(str(iterPatchPath), "rb")

        lg.debug("\tPatching with patchfile {}".format(iterPatchPath))
        patchedDbBytes = bsdiff4.patch(iterDbFile.read(), iterPatchFile.read())

        iterDbFile.close()
        iterPatchFile.close()

        # hash the patched db to see if it eqals the latest db's sha256

        iterDbHasher = hashlib.sha256()
        iterDbHasher.update(patchedDbBytes)
        patchedDbHexDigest = iterDbHasher.hexdigest()

        lg.debug("\t\thex digest (latest) {}".format(latestDbHexDigest))
        lg.debug("\t\thex digest (patch ) {}".format(patchedDbHexDigest))

        if patchedDbHexDigest == latestDbHexDigest:
            lg.info("\tthey match")
        else:
            lg.error("\tHex digests don't match! latest({}) != patched({})".format(latestDbHexDigest, patchedDbHexDigest))
            errorList.append(iterDbTime)
            continue


    if errorList:
        lg.info("These databases had errors: {}".format(pprint.pformat(errorList)))
    else:
        lg.info("no errors found!")

    lg.info("done")








if __name__ == "__main__":
    # if we are being run as a real program

    parser = argparse.ArgumentParser(description="tests the db patches generating the expected databases by using the information in the provided ServerDatabase leveldb database", 
    epilog="Copyright Aug 22, 2014 Mark Grandi")


    parser.add_argument('serverDatabasePath', help="the path to the leveldb database used by the ServerDatabase in parse_gtfs_data.py and all other scripts")
    parser.add_argument("--verbose", "-v", action="store_true", help="increase verbosity")

    logging.basicConfig(level="INFO", format='%(asctime)s %(name)-12s %(levelname)-8s: %(message)s')

    argLog = logging.getLogger("argparse")
    argLog.info("Starting")

    try:
        testPatching(parser.parse_args())
    except Exception:
        argLog.exception("Something wrong happened...")