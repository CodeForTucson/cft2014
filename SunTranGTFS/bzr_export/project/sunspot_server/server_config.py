import leveldb
from enum import Enum

class ConfigEnums:
    ''' this class will hold various full keys or 'prefixes' for leveldb keys
    that we use to get information from the leveldb database'''


    KEY_LAST_DOWNLOAD_TIME = "last_download_time"
    PREFIX_DATABASE_TIME_TO_LOCATION = "t:{}"
    KEY_DB_PATCH_LETTER = "p:"
    PREFIX_DB_PATCH = "{}{}_{}".format(KEY_DB_PATCH_LETTER, "{}", "{}")


class ServerConfig:
    ''' class that wraps around a LevelDB database that holds various stuff about the state of our server,
    what databases we have, what patches we have, what is the latest version of the GTFS data, etc.

    Its not really a config, i should rename it at some point

    You should pass in various ConfigEnums class values to the methods in this class, to avoid magic numbers/values! 
    '''

    def __init__(self, databasePath, logger):
        '''constructor
        @param databasePath - the path to the leveldb database
        @param logger - a logger object'''

        self.db = leveldb.LevelDB(databasePath)
        self.dbFilePath = databasePath
        self.lg = logger.getChild("ServerConfig")



    def __getitem__(self, key):
        '''implementation of obj[item]'''

        

        return self.db.Get(key.encode("utf-8")).decode("utf-8")

    def __setitem__(self, key, value):
        '''implementation of obj[item] = something'''

        self.lg.debug("Setting key: {} , value: {}".format(key, value))

        self.db.Put(key.encode("utf-8"), value.encode("utf-8"))



    def __delitem__(self, key):
        '''implementation of del obj["item"]'''

        self.lg.debug("Deleting key: {}".format(key))
        self.db.Delete(key.encode("utf-8"))

    def __del__(self):
        ''' destructor'''

        # according to the source code for py-leveldb, when the LevelDB object gets deallocated it
        # closes the database normally. Even in c++ you don't really close the database, you just call del *db
        # and it closes automatically

        del self.db

        self.lg.debug("ServerConfig.__del__() called, closing database at {}".format(self.dbFile ))


    def setWithPrefix(self, key, formatEntries, value):
        ''' sets a @value with the @key formatted with @formatEntries (should be something from ConfigEnums) 

        @param key - the key to set in the leveldb database, that has some format markers ({})
        @param formatEntries - an iterable to use when we call .format() on @key
        @param value - the value to set as the value for @key in the leveldb database
        '''

        # don't do any encoding or decoding, the __setitem__ method does that

        self[key.format(*formatEntries)] = value

    def getWithPrefix(self, key, formatEntries):
        ''' returns a @value with the @key formatted with @formatEntries (should be something from ConfigEnums )

        @param key - the key to use to retrieve a value from the leveldb database, that has some format markers ({})
        @param formatEntries - an iterable to use when we call .format() on @key
        @return a tuple of bytearrays
        '''

        # don't do any encoding or decoding, the __getitem__ method does that
        return self[key.format(*formatEntries)]

    def getGeneratorWithPrefix(self, key, formatEntries):
        ''' a generator that yields entries from RangeIter as long as the keys startwith the key.format(*formatEntries)

        with the @key formatted with @formatEntries (should be something from ConfigEnums )

        pass in a iterable of empty strings if you are searching by prefix (like you have c:1, c:2, c:3 and you 
            want to search by 'c:' )

        @param key - the key to use to start the leveldb search from, that has some format markers ({})
            will also be used to see if the keys the database returns start with this string (after format is called),
            if they don't startwith(@key), then we stop the iteration
        @param formatEntries - an iterable we use when we call .format() on @key
        '''
        #self.lg.debug("key: {}, formatEntries: {}".format(key, formatEntries))

        thekey = key.format(*formatEntries)

        for iterResult in self.db.RangeIter(thekey.encode("utf-8")):
            if iterResult[0].decode("utf-8").startswith(thekey):
                yield iterResult
            else:
                break


    def deleteAllInRange(self, rangeStr, formatEntries):
        ''' deletes all the keys that we get back from a RangeIter() call with the
        rangeStr parameter

        rangeStr will be formatted with @formatEntries (should be something from ConfigEnums)

        @param rangeStr - the key to use to start the leveldb search from (to delete), that has some format markers ({})
            will also be used to see if the keys the database returns start with this string (after format is called),
            if they don't startwith(@rangeStr), then we stop the iteration
        @param formatEntries - an iterable we use when we call .format() on @rangeStr
        '''

        toDelList = list()
        for iterResult in self.getGeneratorWithPrefix(rangeStr, formatEntries):
            toDelList.append(iterResult[0])

        for iterEntry in toDelList:
            self.lg.debug("Deleting key {}".format(iterEntry))
            self.db.Delete(iterEntry)







