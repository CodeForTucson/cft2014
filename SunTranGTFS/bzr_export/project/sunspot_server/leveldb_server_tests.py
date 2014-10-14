#!/usr/bin/env python3

import unittest
from server_database import ServerDatabase
import random
import logging

ipAddr = "127.0.0.1"
port = "8888"

alphabet = "a b c d e f g h i j k l m n o p q r s t u v w x y z 0 1 2 3 4 5 6 7 8 9 ! @ # $ % ^ & * ( ) _ + = -".split(" ")


class TestLeveldbServer(unittest.TestCase):
    ''' note that test methods must start with the word 'test' '''


    # GET = 0;
    # SET = 1;
    # DELETE = 2;
    # START_RANGE_ITER = 3;
    # DELETE_ALL_IN_RANGE = 4;
    # RETURN_ATONCE_RANGE_ITER = 5;

    def _log(self, message, level=logging.DEBUG):
        TestLeveldbServer.lg.log(level, message)

    @classmethod
    def setUpClass(cls):
        cls.lg = logging.getLogger("UnitTest")
        logging.basicConfig(level="INFO")
        cls.serverDb = ServerDatabase(ipAddr, port, cls.lg.getChild("ServerDatabase"))

    @classmethod
    def tearDownClass(cls):

        cls.serverDb = None

    
    def testGetAndSet(self):

        sdb = TestLeveldbServer.serverDb

        for idx in range(5):

            randomKey = "".join(random.sample(alphabet, 5))
            randomValue = "".join(random.sample(alphabet, 10))

            self._log("random key/value is: {} / {}".format(randomKey, randomValue))
            sdb[randomKey] = randomValue

            self.assertEqual(sdb[randomKey], randomValue)

    def testGetKeyError(self):

        sdb = TestLeveldbServer.serverDb


        for idx in range(5):

            randomKey = "SHOULDNOTBEHERE_" + "".join(random.sample(alphabet, 5))

            with self.assertRaises(KeyError):

                self._log("asserting that key {} raises keyerror".format(randomKey))
                sdb[randomKey]


    def testSetAndDelete(self):

        sdb = TestLeveldbServer.serverDb

        for idx in range(5):

            randomKey = "".join(random.sample(alphabet, 5))
            randomValue = "".join(random.sample(alphabet, 10))

            # set the value
            self._log("Setting {} and then deleting it".format(randomKey))
            sdb[randomKey] = randomValue

            # assert it set correctly
            self.assertEqual(sdb[randomKey], randomValue)

            # delete it

            del sdb[randomKey]

            # assert keyerror when we get it again
            with self.assertRaises(KeyError):
                sdb[randomKey]

    def testDeleteAllInRange(self):

        sdb = TestLeveldbServer.serverDb

        prefix = "HELLOTHISISAPREFIX_"

        thelist = []

        for idx in range(5):

            randomKey = prefix + "".join(random.sample(alphabet, 5))
            randomValue = "".join(random.sample(alphabet, 10))

            # add the key with the prefix
            sdb[randomKey] = randomValue
            thelist.append(randomKey)

            # ensure that it is there
            self.assertEqual(sdb[randomKey], randomValue)

        self._log("deleting all in range {}".format(prefix), logging.INFO)
        # then delete all in range (with the prefix)
        sdb.deleteAllInRange("{}", [prefix])

        for entry in thelist:
            # ensure that we get key errors for all of these keys we added

            with self.assertRaises(KeyError):
                self._log("\ttesting {}".format(entry), logging.INFO)
                sdb[entry]


    def testReturnAtOnceIter(self):

        sdb = TestLeveldbServer.serverDb

        prefix = "HELLO_"

        theDict = dict()

        # insert 5 entries that have the prefix
        for idx in range(5):
            randomKey = prefix + "".join(random.sample(alphabet, 5))
            randomValue = "".join(random.sample(alphabet, 10))

            # add the key with the prefix
            sdb[randomKey] = randomValue
            theDict[randomKey] = randomValue

            # ensure that it is there
            self.assertEqual(sdb[randomKey], randomValue)

        # insert 5 more random values
        for idx in range(5):
            randomKey =  "".join(random.sample(alphabet, 5))
            randomValue = "".join(random.sample(alphabet, 10))

            # add the key with the prefix
            sdb[randomKey] = randomValue
            theDict[randomKey] = randomValue


            # ensure that it is there
            self.assertEqual(sdb[randomKey], randomValue)

        # do a atOnceIter
        for key,value in sdb.getGeneratorWithPrefix("{}", [prefix]):
            #print("asserting {} and {}".format(theDict[key], value))
            self.assertEqual(theDict[key], value)






# run the unit tests
if __name__ == '__main__':
    unittest.main()