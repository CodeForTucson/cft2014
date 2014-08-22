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
    ''' class that just handles loading and saving our server config and whatnot
    '''

    def __init__(self, databasePath, logger):
        '''constructor'''

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
        '''

        # don't do any encoding or decoding, the __setitem__ method does that

        self[key.format(*formatEntries)] = value

    def getWithPrefix(self, prefix, formatEntries):
        ''' returns a @value with the @key formatted with @formatEntries (should be something from ConfigEnums )
        '''

        # don't do any encoding or decoding, the __getitem__ method does that
        return self[prefix.format(*formatEntries)]

    def getGeneratorWithPrefix(self, key, formatEntries):
        ''' a generator that yields entries from RangeIter as long as the keys startwith the key.format(*formatEntries)

        with the @key formatted with @formatEntries (should be something from ConfigEnums )

        pass in a iterable of empty strings if you are searching by prefix (like you have c:1, c:2, c:3 and you 
            want to search by 'c:' )
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
        '''

        toDelList = list()
        for iterResult in self.getGeneratorWithPrefix(rangeStr, formatEntries):
            toDelList.append(iterResult[0])

        for iterEntry in toDelList:
            self.lg.debug("Deleting key {}".format(iterEntry))
            self.db.Delete(iterEntry)







