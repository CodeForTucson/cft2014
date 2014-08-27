#
# simple script that can be used to test the leveldb server. 
# based off the protocol example in the python documentation
#
# Mark Grandi - Aug 27, 2014
#


import asyncio

import json
import random
class EchoClient(asyncio.Protocol):
    message = 'This is the message. It will be echoed.'

    def connection_made(self, transport):

        self.transport = transport
        msg = json.dumps({"type": "get", "key": "thisisakey2", "value": "thisisavalue"})
        transport.write(msg.encode("utf-8"))
        print('data sent: {}'.format(msg))

    def data_received(self, data):
        print('data received: {}'.format(data.decode()))


        yield from asyncio.sleep(5)
        msg = json.dumps({"type": "get", "key": "thisisakey2", "value": "thisisavalue"})

        self.transport.write(msg.encode("utf-8"))

    def connection_lost(self, exc):
        print('server closed the connection')
        asyncio.get_event_loop().stop()

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
    while True:
        print("outer loop start")

        result = random.choice([0,1])

        if result == 0 and len(listOfKeysCreated) != 0:
            print("get")
            tmpkey = random.choice(listOfKeysCreated)
            msg = json.dumps({"type": "get", "key": tmpkey, "value": ""})
            print("asking for value for key '{}'".format(tmpkey))

        else:
            print("set")
            tmpkey = "".join(random.sample(alphabet, 10))
            listOfKeysCreated.append(tmpkey)
            value = "".join(random.sample(alphabet, 5))
            print("creating key '{}' with value '{}'".format(tmpkey, value))
            msg = json.dumps({"type": "set", "key": tmpkey, "value": value})

        print("writing {}".format(msg))
        writer.write(msg.encode("utf-8"))

        
        tmpdata = yield from reader.read(8192)
        print("reading, got {}".format(tmpdata))

        if not tmpdata:
            print("done reading, breaking")
            break

        print("sleeping")
        print("\n\n\n")
        yield from asyncio.sleep(5)


loop = asyncio.get_event_loop()
task = asyncio.async(repeat())
loop.run_until_complete(task)
loop.close()