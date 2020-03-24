import socket
import multiprocessing
from struct import unpack,pack
from xmlrpc.server import SimpleXMLRPCServer
import time
import logging
import sys,json,os

'''#Prasing commandline arguments
configFile=sys.argv[1]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)'''


host="0.0.0.0"
port=int(sys.argv[1])

datastore_host=""
datastore_port=7000

reduce_data=int(sys.argv[2])

lock=multiprocessing.Lock()
server = SimpleXMLRPCServer((host, port), logRequests=True,allow_none=True)

# Creating a log file
logging.basicConfig(filename='home/adarshhegdegef/cloud_reducer.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

def reducer_wordcount(input_data):

    s=input_data.replace("(","").split(')')
    s=[i for i in s if i!='']
    unique=sorted(list(set(s)))
    res=''
    for i in unique:
        word=i.split(':')
        res+="("+str(word[0])+":"+str(s.count(i))+")"

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")


    req="set WORD_COUNT "+str(len(res))+"\r\n"+str(res)+"\r\n"

    logging.debug("Storing Reduce output from: {0}".format(os.getpid()))
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    soc.close()
    return data.decode()


def reducer_invertedindex(input_data):
    input_data=input_data.strip()

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="set INVERTED_INDEX "+str(len(input_data))+"\r\n"+str(input_data)+"\r\n"

    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    soc.close()
    return data.decode()

def getData(reduce_fn):
    logging.debug("Getting Input Data")
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    if reduce_fn=='WORD_COUNT_REDUCE':
        req="get COUNT_SHUFFLED\r\n"
    elif reduce_fn=='INVERTED_INDEX_REDUCE':
        req="get INVERTED_SHUFFLED\r\n"
    else:
        print("Reduce function not supported")

    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    ln=soc.recv(8)
    (length,)=unpack('>Q',ln)
    data=b''
    while len(data)< length:
        to_Read=length-len(data)
        data+=soc.recv(1024 if to_Read > 1024 else to_Read) #Referred from stackoverflow

    input_data=data.decode()
    input_data = input_data.split("\n",2)[1]
    logging.debug("Input Data Received")

    if reduce_fn=='WORD_COUNT_REDUCE':
        input_data=input_data.split(',')
        input_data.pop()
    elif reduce_fn=='INVERTED_INDEX_REDUCE':
        input_data=input_data.split('~~')

    input_data=input_data[reduce_data]
    print("input data:",input_data)
    soc.close()
    return input_data


def word_count_reducer(reduce_fn,host,port):
    logging.info(host)
    logging.info(port)
    global datastore_host
    datastore_host=host
    global datastore_port
    datastore_port=port
    input_data=getData(reduce_fn)
    response=''
    if input_data:
        if reduce_fn=='WORD_COUNT_REDUCE':
            response=reducer_wordcount(input_data)
        elif reduce_fn=='INVERTED_INDEX_REDUCE':
            response=reducer_invertedindex(input_data)
        if response:
            return "REDUCER_DONE"


print("Word Count Reducer Started...")
logging.info("Word Count Reducer Started...")
server.register_function(word_count_reducer)
server.serve_forever()
