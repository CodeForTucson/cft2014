#
# simple script that can be used to test the leveldb server. 
# based off the protocol example in the python documentation
#
# Mark Grandi - Aug 27, 2014
#


import asyncio

import json
import random
import arrow

import sys
sys.path.append("../project/sunspot_server")

from leveldb_server_messages_pb2 import LeveldbServerMessages


# class EchoClient(asyncio.Protocol):
#     message = 'This is the message. It will be echoed.'

#     def connection_made(self, transport):

#         self.transport = transport
#         msg = json.dumps({"type": "get", "key": "thisisakey2", "value": "thisisavalue"})
#         transport.write(msg.encode("utf-8"))
#         print('data sent: {}'.format(msg))

#     def data_received(self, data):
#         print('data received: {}'.format(data.decode()))


#         yield from asyncio.sleep(5)
#         msg = json.dumps({"type": "get", "key": "thisisakey2", "value": "thisisavalue"})

#         self.transport.write(msg.encode("utf-8"))

#     def connection_lost(self, exc):
#         print('server closed the connection')
#         asyncio.get_event_loop().stop()

# loop = asyncio.get_event_loop()
# coro = loop.create_connection(EchoClient, '127.0.0.1', 8888)
# loop.run_until_complete(coro)
# loop.run_forever()
# loop.close()


alphabet = "a b c d e f g h i j k l m n o p q r s t u v w x y z 0 1 2 3 4 5 6 7 8 9 ! @ # $ % ^ & * ( ) _ + = -".split(" ")
@asyncio.coroutine
def repeat():

    reader, writer = yield from asyncio.open_connection("127.0.0.1", 8888)

    listOfKeysCreated = list()

    print("starting")


    # these two things are used to make it so that delete all in range deletes more then one thing
    tmpPrefix = "".join(random.sample(alphabet, 2))
    prefixCounter = 0
    while True:
        # print("outer loop start")

        protoObj = LeveldbServerMessages.ActualData()
        protoObj.timestamp = arrow.now().timestamp # int64
        protoObj.type = LeveldbServerMessages.ActualData.QUERY
        query = protoObj.query

        result = random.choice([0,0, 0,1, 1, 1, 1, 1,2,3,4, 4, 4])
        skip = False

        if result == 0 and len(listOfKeysCreated) != 0:
            print("*** GET ***")
            query.type = LeveldbServerMessages.ServerQuery.GET


            tmpkey = random.choice(listOfKeysCreated)
            query.key = tmpkey.encode("utf-8")
            #msg = json.dumps({"type": "get", "key": tmpkey, "value": ""})
            print("asking for value for key '{}'".format(tmpkey))

        elif result == 1:
            print("*** SET ***")
            query.type = LeveldbServerMessages.ServerQuery.SET




            tmpkey = tmpPrefix + "".join(random.sample(alphabet, 10))
            listOfKeysCreated.append(tmpkey)
            value = "".join(random.sample(alphabet, 5))

            query.key =  tmpkey.encode("utf-8")
            query.value = value.encode("utf-8")

            prefixCounter += 1
            if prefixCounter >= 5:
                tmpPrefix = "".join(random.sample(alphabet, 2))
                prefixCounter = 0

            print("creating key '{}' with value '{}'".format(tmpkey, value))
            # msg = json.dumps({"type": "set", "key": tmpkey, "value": value})

        elif result == 2:

            if len(listOfKeysCreated) != 0:
                print("*** DELETE ***")

                query.type = LeveldbServerMessages.ServerQuery.DELETE

                thechoice = random.randint(0, len(listOfKeysCreated) - 1)
                query.key = listOfKeysCreated.pop(thechoice).encode("utf-8")

                print("Deleting key '{}'".format(query.key))
            else:
                print("skipping..")
                skip = True

        elif result == 3:
            if len(listOfKeysCreated) != 0:
                print("*** DELETE ALL IN RANGE ***")

                query.type = LeveldbServerMessages.ServerQuery.DELETE_ALL_IN_RANGE

                thechoice = random.randint(0, len(listOfKeysCreated) - 1)
                tmpkey = listOfKeysCreated.pop(thechoice)

                query.key = tmpkey[:2].encode("utf-8")
                print("deleting keys with prefix {}".format(query.key))

                for idx, iterKey in enumerate(listOfKeysCreated):
                    if iterKey.startswith(tmpkey[:2]):
                        print("\tshould delete key: {}".format(listOfKeysCreated.pop(idx)))

            else:
                print("skipping..")
                skip = True
        else:
            if len(listOfKeysCreated) != 0:
                print("*** RETURN ALL IN RANGE ***")


                query.type = LeveldbServerMessages.ServerQuery.RETURN_ATONCE_RANGE_ITER

                tmpstr = random.choice(listOfKeysCreated)

                query.key = tmpstr[:2].encode("utf-8")

                print("returning all keys with prefix: {}".format(query.key))                


            else:
                print("skipping..")
                skip = True

        if protoObj.IsInitialized() and not skip:
            skip = False
            protoObjBytes = protoObj.SerializeToString()

            # add the length prefix

            lenPrefix = len(protoObjBytes).to_bytes(2, "big")
            print("writing {}, {}".format(lenPrefix + protoObjBytes, protoObj))
            writer.write(lenPrefix + protoObjBytes)

            
            tmpdata = yield from reader.read(8192)
            print("reading, got {}".format(tmpdata))

            try:
                respProto = LeveldbServerMessages.ActualData.FromString(tmpdata)
                print("proto obj: {}".format(respProto))
            except:
                pass

            if not tmpdata or reader.at_eof():
                print("done reading, breaking")
                break

            print("sleeping")
            print("***********")
            yield from asyncio.sleep(3)
        else:
            print("protobuf obj was not initalized")


loop = asyncio.get_event_loop()
task = asyncio.async(repeat())
loop.run_until_complete(task)
loop.close()