from xmlrpc.server import SimpleXMLRPCServer
import multiprocessing
import os
import socket
from struct import pack,unpack
import re
import time
import logging
import sys
import json



host="0.0.0.0"
port=int(sys.argv[1])

datastore_host="0.0.0.0"
datastore_port=7000

map_data=int(sys.argv[2])

lock=multiprocessing.Lock()
server = SimpleXMLRPCServer((host, port), logRequests=True,allow_none=True)

# Creating a log file
logging.basicConfig(filename='home/adarshhegdegef/cloud_mapper.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


def mapper_wordcount(input_data):
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    regex = r'\w+'
    words=re.findall(regex,input_data)
    mp=''
    for i in words:
        mp=mp+"("+str(i)+":"+"1"+")"+","
    req="set MAP_COUNT "+str(len(mp))+"\r\n"+str(mp)+"\r\n"
    length=pack('>Q',len(req))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    soc.close()
    return data.decode()


def mapper_invertedindex(input_data):

    regex = r'\w+'
    words=re.findall(regex,input_data)
    unique=sorted(list(set(words)))
    pd="document_mapper_"+str(map_data)
    res=''
    for i in unique:
        res+="("+str(i)+":~"+str(pd)+":"+str(words.count(i))+")"

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="set MAP_INVERTED "+str(len(res))+"\r\n"+str(res)+"\r\n"


    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    soc.close()
    return data.decode()


def getData(map_fn):
    logging.debug("Getting Input Data")
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    if(map_fn=='WORD_COUNT_MAP'):
        req="get MAP_INPUT\r\n"
    elif(map_fn=='INVERTED_INDEX_MAP'):
        req="get INVERTED_INPUT\r\n"
    else:
        print("Map not supported")

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
    input_data=input_data.split('~~')
    input_data.pop()
    input_data=input_data[map_data]
    soc.close()
    return input_data



def word_count_mapper(map_fn,host,port):
    logging.info(host)
    logging.info(port)
    global datastore_host
    datastore_host=host
    global datastore_port
    datastore_port=port
    input_data=getData(map_fn)
    response=''
    if input_data:
        if map_fn=='WORD_COUNT_MAP':
            response=mapper_wordcount(input_data)
        elif map_fn=='INVERTED_INDEX_MAP':
            response=mapper_invertedindex(input_data)

        if response:
            return "MAP_DONE"
        else:
            return "MAP_FAILED"


print("Word Count Mapper Started...")
logging.info("Word Count Mapper Started...")
server.register_function(word_count_mapper)
server.serve_forever()

