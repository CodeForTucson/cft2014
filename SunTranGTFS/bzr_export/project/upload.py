#
#
# Script to automate uploading files to my server
#
# written by Mark Grandi - Aug 12, 2014
#

import argparse, sys, yaml, platform, logging, pathlib, subprocess, os, os.path

def uploadFiles(args):
    '''uploads files according to rules in a yaml config file
    @param args - the namespace object we get from argparse.parse_args()
    '''

    # args.configFile is a tuple, first is the filepath second is the yaml object
    config = args.configFile[1]["upload_config"]

    logging.basicConfig(level="INFO", format='%(asctime)s %(name)-12s %(levelname)-8s: %(message)s')
    logger = logging.getLogger("main")
    if args.verbose:
        logger.setLevel("DEBUG")

    # figure out what we are doing
    runProtoc = True
    runUpload = True

    # since the 'onlyRunXXX' arguments are optional
    # if they are both false then we are just running everything
    if args.onlyRunProtoc ^ args.onlyRunUpload: # note, ^ is xor! 

        # we are only running one specific thing
        runProtoc = args.onlyRunProtoc
        runUpload = args.onlyRunUpload

    logger.info("Running protoc: {}".format(runProtoc))
    logger.info("Running upload: {}".format(runUpload))


    ###################
    # SETTING UP (what binaries to use)
    ###################

    # a dictionary with the binary names that we use , depending on the platform
    binNamesDict = None

    # we need to make sure we are using the currentDir of the config file as thats where all the paths are going to be 
    # relative to
    currentDir = pathlib.Path(args.configFile[0]).parent
    logger.info("Working directory is {}".format(currentDir))

    # first see what operating system we are on
    osStr = platform.system()
    logger.info("platform is: '{}'".format(osStr))

    if osStr in config["binaries"].keys():
        binNamesDict = config["binaries"][osStr]
        logger.debug("Binary names that we are using: {}".format(binNamesDict))
    else:
        logger.critical("Unknown platform, can't figure out what binaries to use! It was: {}, but expected {}"
            .format(osStr, config["binaries"].keys()))
        sys.exit()

    ###################
    # now run the protoc stuff
    ###################

    if runProtoc:

        # here we try and delete the 'old' protobuf generated file before we generate a new one.

        dictOfProtocEntries = config["protoc_list"]

        protoLogger = logger.getChild("protoc")
        protoLogger.info("Starting Protocol Buffer generation")

        # go through each of the entries we need to run protoc on
        for iterKey in dictOfProtocEntries.keys():

            protoLogger.info("on entry '{}'".format(iterKey))

            iterDict = dictOfProtocEntries[iterKey]

            finalPb2Name = os.path.splitext(os.path.basename(iterDict["filepath"]))[0] + "_pb2.py"
            outputProtoFile = currentDir / pathlib.Path(iterDict["output_path"]) / pathlib.Path(finalPb2Name)
            protoLogger.debug("Removing old file: {}".format(outputProtoFile))


            # check to see if the file exists, if it does, delete it.
            try:
                if not outputProtoFile.exists():
                    protoLogger.debug("old file doesn't exist, skipping")
                else:
                    os.remove(str(outputProtoFile))
            except Exception as e:
                protoLogger.exception("Error when trying to remove file {}".format(outputProtoFile))
                sys.exit()

            # run the protoc command
            tmpCmd = [
                binNamesDict["protoc_bin"],
                "-I={}".format(iterDict["include_path"]),
                iterDict["filepath"], 
                "--python_out={}".format(iterDict["output_path"])
                ]

            protoLogger.debug("Running command: {}".format(tmpCmd))

            try:
                # have stderr redirect to stdout so if we get errors we actually get a message rather then empty string
                # for the output
                subprocess.check_output(tmpCmd, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                protoLogger.exception("Error calling protoc command: '{}': returncode: {}, output: {}"
                    .format(tmpCmd, e.returncode, e.output))
                sys.exit()


    ###################
    # Start the uploading
    ###################

    if runUpload:
        uploadLogger = logger.getChild("upload")
        uploadLogger.info("Starting Uploading")

        uploadEntryDict = config["upload_list"]

        for iterUploadEntryKey in uploadEntryDict.keys():

            uploadLogger.info("On entry {}".format(iterUploadEntryKey))

            uploadInfoDict = uploadEntryDict[iterUploadEntryKey]

            fileToUpload = pathlib.Path(uploadInfoDict["filepath"])
            # make sure the file exists first
            if not fileToUpload.exists():
                uploadLogger.error("ERROR: Could not upload, the file doesn't exist...({})".format(fileToUpload))
                continue

            # here we are taking the upload_to path and comparing it to the rsync_module_path
            # that way if the rsync_module_path is /something, and the upload_to is /something/hello,
            # then we only put /hello in the destination url
            #
            # the longer path should be the one having 'relative_to' called on it
            relativeToRsyncModulePath = pathlib.PurePosixPath(uploadInfoDict["upload_to"]).relative_to(config["rsync_module_path"])

            uploadLogger.debug("path relative to the rsync module's path is: {}".format(relativeToRsyncModulePath))

            # populate the command
            # TODO: maybe have support for connecting via rsync daemon or ssh?
            # since if we are using rsync daemon, it can automatically chown the stuff for us (configure 'gid' and 'uid'
            # for the module in rsyncd.conf ) but then sometimes maybe we want to chown it to something different? 
            # or rsync something outside of the module's path?
            #
            # EL OH EL
            # using 'rsync://' in the destination path apparently makes subprocess mangle the fucking thing to all hell
            # so i'm using double colons, as the rsync manpage says 'you either use a double colon :: instead of a  single  
            # colon to separate the hostname from the path, or you use an rsync:// URL.'
            uploadCmd = [
                binNamesDict["rsync_bin"], 
                "-q", 
                str(fileToUpload), 
                "--password-file",
                "{}".format(config["rsync_passwd_file"]),  
                "{}::{}/{}".format(config["remote_url"], config["rsync_module_name"], relativeToRsyncModulePath )
                ]

            uploadLogger.debug("Running command (rsync): '{}'".format(uploadCmd))

            try:
                subprocess.check_call(uploadCmd)
            except subprocess.CalledProcessError as e:
                uploadLogger.exception("Error calling rsync command: '{}': returncode: {}, output: {}"
                    .format(uploadCmd, e.returncode, e.output))
                sys.exit()


            #*************************
            # CHOWNING SECTION (underneath uploading)
            #*************************

            # now chown the file to be whatever it is in the config file
            # or else the server can't access the filesss
            #
            # don't chown anything if the string is an empty string
            if uploadInfoDict["chown_to"]:
                sshCmd = [x for x in binNamesDict["ssh_cmd"]]

                sshCmd.append("{}".format(config["remote_url"]))

                # like rsync's --rsh, the command we are running needs to be one 'entry' in the list so the shell doesn't mangle
                # see http://stackoverflow.com/a/12496107/975046
                # quotes and stuff
                sshCmd.append("chown {} {}".format(
                        uploadInfoDict["chown_to"], 
                        pathlib.Path(uploadInfoDict["upload_to"], pathlib.Path(uploadInfoDict["filepath"]).name)) )

                uploadLogger.debug("Running command (ssh/chown): {}".format(sshCmd))

                try:
                    subprocess.check_call(sshCmd, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
                except subprocess.CalledProcessError as e:
                    uploadLogger.exception("Error calling ssh/chown command: '{}': returncode: {}, output: {}"
                        .format(sshCmd, e.returncode, e.output))
                    sys.exit()



    logger.info("done")







def isYamlTypeWithFiletype(stringArg):
    ''' helper method for argparse that sees if the argument is a valid yaml file
    @param stringArg - the argument we get from argparse
    @return a tuple, first is the pathlib.Path object of the filepath, second is the the loaded file if it is indeed a valid yaml file, 
        or raises ArgumentTypeError if its not. '''

    try:
        with open(stringArg, encoding="utf-8") as f:
            test=yaml.safe_load(f)
        # if it succeeds, then return the yaml value AS A TUPLE WHERE THE FIRST VALUE IS THE FILEPATH 
        return (pathlib.Path(stringArg).resolve(), test,)
    except yaml.YAMLError as e:
        if hasattr(e, "problem_mark"):
            mark=e.problem_mark
            raise argparse.ArgumentTypeError("Problem parsing yaml: error was at {}:{}".format(mark.line+1, mark.column+1))
    except OSError as e2:
        raise argparse.ArgumentTypeError("error reading file at {}, error was {}".format(stringArg, e2))
    except Exception as e3:
        raise argparse.ArgumentTypeError("general error: {}".format(e3))

if __name__ == "__main__":
    # if we are being run as a real program

    parser = argparse.ArgumentParser(description="uploads files according to rules in a yaml config file", 
    epilog="Copyright Aug 12, 2014 - Mark Grandi")

    # optional arguments, if specified these are the input and output files, if not specified, it uses stdin and stdout
    parser.add_argument('configFile', type=isYamlTypeWithFiletype, help="the config file to use")
    parser.add_argument('--verbose', "-v", action="store_true", help="increase verbosity")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--only-run-protoc", dest="onlyRunProtoc", action="store_true", help="only run protoc generation")
    group.add_argument("--only-run-upload", dest="onlyRunUpload", action="store_true", help="only run uploads")
    
    uploadFiles(parser.parse_args())