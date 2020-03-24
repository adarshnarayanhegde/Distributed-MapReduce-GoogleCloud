#Instance handling codes in this file are referred from google cloud documents

import googleapiclient
import time
import googleapiclient.discovery
from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from lxml import etree
import xmlrpc.client as xc
import requests,math
import textwrap
import socket
from struct import pack,unpack
import json
import sys
import logging
import cgi
import io
from http.server import BaseHTTPRequestHandler,HTTPServer
import time,json

datastore_host=""
datastore_port=7000

mapper1_host=""
mapper1_port=7002

mapper2_host=""
mapper2_port=7003

mapper3_host=""
mapper3_port=7004

reducer1_host=""
reducer1_port=7005

reducer2_host=""
reducer2_port=7006

reducer3_host=""
reducer3_port=7007


#*********************************************Insatance Handling Section***************************************************#
# To get a list of instances
def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None


#Creating Instances as Required
def create_instance(compute, project, zone, name, bucket, startup_script):
    # Get the latest Debian Jessie image.
    image_response = compute.images().getFromFamily(
        project='debian-cloud', family='debian-9').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],
        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/compute',
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script-url',
                'value': startup_script
            }, {
                'key': 'bucket',
                'value': bucket
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()
# [END create_instance]



#For deleting an instance
# [START delete_instance]
def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
# [END delete_instance]


#For waiting until creation of VM
# [START wait_for_operation]
def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)
# [END wait_for_operation]


#To start an existing instance
def start_instance(compute, project, zone, instance):

    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = project
    zone = zone
    instance = instance

    request = service.instances().start(project=project, zone=zone, instance=instance)
    response = request.execute()

    pprint(response)
    return compute.instances().start(
        project=project,
        zone=zone,
        instance=instance).execute()


# To stop an existing instance
def stop_instance(compute, project, zone, instance):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = project
    zone = zone
    instance = instance

    request = service.instances().stop(project=project, zone=zone, instance=instance)
    response = request.execute()

    pprint(response)
    return compute.instances().stop(
        project=project,
        zone=zone,
        instance=instance).execute()


# To get IP of an instance
def get_instance(compute, project, zone, instance):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = project
    zone = zone
    instance = instance

    request = service.instances().get(project=project, zone=zone, instance=instance)
    response = request.execute()

    pprint(response)
    return compute.instances().get(
        project=project,
        zone=zone,
        instance=instance).execute()


#*********************************************End of Insatance Handling Section***************************************************#


#****************************************************Map Reduce Section***********************************************************#

# Creating a log file
logging.basicConfig(filename='/home/adarshhegdegef/master.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


'''ipConfig=sys.argv[1]
ipconf={}

with open(ipConfig) as jsonFile:
    ipconf=json.load(jsonFile)'''


proxy_mapper1 = xc.ServerProxy("http://%s:%s"%(mapper1_host,mapper1_port),allow_none=True)
proxy_mapper2 = xc.ServerProxy("http://%s:%s"%(mapper2_host,mapper2_port),allow_none=True)
proxy_mapper3 = xc.ServerProxy("http://%s:%s"%(mapper3_host,mapper3_port),allow_none=True)

proxy_reducer1 = xc.ServerProxy("http://%s:%s"%(reducer1_host,reducer1_port),allow_none=True)
proxy_reducer2= xc.ServerProxy("http://%s:%s"%(reducer2_host,reducer2_port),allow_none=True)
proxy_reducer3= xc.ServerProxy("http://%s:%s"%(reducer3_host,reducer3_port),allow_none=True)


#Function to validate configurations
def validate_config(input_data,map_fn,reduce_fn,output_location):
    if not input_data:
        logging.error("No input data received")
        return "No input data received!"
        #exit(0)
    if map_fn not in ['wordCountMapper','invertedIndexMapper']:
        logging.error("Map Function not supported")
        return "Map Function currently not supported!"
        #exit(0)
    if reduce_fn not in ['wordCountReducer','invertedIndexReducer']:
        logging.error("Reduce Function not supported")
        return "Reduce Function currently not supported!"
        #exit(0)
    if not output_location:
        logging.error("Output location not provided")
        return "Output location not provided!"
        #exit(0)
    return ""


#Prasing commandline arguments
'''configFile=sys.argv[2]
config={}

with open(configFile) as jsonFile:
    config=json.load(jsonFile)

validate_config(config)'''


#Function to store input data into key-value store
def store_input(map_fn,input_data):

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req=''
    if map_fn=='wordCountMapper':
        response = requests.get(input_data)
        html = etree.HTML(response.text)
        text = '\n'.join([el.text for el in html.findall('.//p')])
        ln=math.floor(len(text)/3)
        res=textwrap.wrap(text, ln)

        split=''
        for i in res:
            split+=i+'~~'

        req="set"+" "+str("MAP_INPUT")+" "+str(len(split))+"\r\n"+str(split)+"\r\n"

    elif map_fn=='invertedIndexMapper':
        text=''
        links=input_data
        for i in links:
            response = requests.get(i)
            html = etree.HTML(response.text)
            text += '\n'.join([el.text for el in html.findall('.//p')])+"~~"

        req="set"+" "+str("INVERTED_INPUT")+" "+str(len(text))+"\r\n"+str(text)+"\r\n"

    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    data=data.decode().strip()
    soc.close()
    return data


#Function to invoke map tasks
def invoke_map(map_fn,mapper1_host,mapper2_host,mapper3_host):

    proxy_mapper1 = xc.ServerProxy("http://%s:%s"%(mapper1_host,mapper1_port),allow_none=True)
    proxy_mapper2 = xc.ServerProxy("http://%s:%s"%(mapper2_host,mapper2_port),allow_none=True)
    proxy_mapper3 = xc.ServerProxy("http://%s:%s"%(mapper3_host,mapper3_port),allow_none=True)

    logging.info("Mapper proxy")
    logging.info("http://%s:%s"%(mapper1_host,mapper1_port))
    logging.info("http://%s:%s"%(mapper2_host,mapper2_port))
    logging.info("http://%s:%s"%(mapper3_host,mapper3_port))

    if map_fn=='wordCountMapper':
        try:
            print("Running Word Count Mapper...")
            logging.debug("RPC Call to Word Count Mapper")
            map=proxy_mapper1.word_count_mapper("WORD_COUNT_MAP",datastore_host,datastore_port)
            print("mapper1:",map)
            map=proxy_mapper2.word_count_mapper("WORD_COUNT_MAP",datastore_host,datastore_port)
            print("mapper2:",map)
            map=proxy_mapper3.word_count_mapper("WORD_COUNT_MAP",datastore_host,datastore_port)
            print("mapper3:",map)
            return map
        except:
            logging.error("RPC Call to Word Count Mapper Failed")
    elif map_fn=='invertedIndexMapper':
        try:
            print("Running Inverted Index Mapper...")
            logging.debug("RPC Call to Inverted Index Mapper")
            map=proxy_mapper1.word_count_mapper("INVERTED_INDEX_MAP",datastore_host,datastore_port)
            print("mapper1:",map)
            map=proxy_mapper2.word_count_mapper("INVERTED_INDEX_MAP",datastore_host,datastore_port)
            print("mapper2:",map)
            map=proxy_mapper3.word_count_mapper("INVERTED_INDEX_MAP",datastore_host,datastore_port)
            print("mapper3:",map)
            return map
        except:
            logging.error("RPC Call to Inverted Index Mapper Failed")
    else:
        print("Map Function Not Supported")
        logging.error("Map Function Not Supported")


#Function to shuffle map outputs
def invoke_shuffle(reduce_fn):

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    if reduce_fn=='wordCountReducer':
        print("Running Word Count Shuffle Task...")
        logging.debug("Running Word Count Shuffle Task...")
        req="shuffle MAP_COUNT\r\n"
    elif reduce_fn=='invertedIndexReducer':
        print("Running Inverted Index Shuffle Task...")
        logging.debug("Running Inverted Index Shuffle Task...")
        req="shuffle MAP_INVERTED\r\n"

    length=pack('>Q',len(req))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    data=data.decode().strip()
    soc.close()
    return data


#Function to invoke reduce tasks
def invoke_reduce(reduce_fn,reducer1_host,reducer2_host,reducer3_host):

    proxy_reducer1 = xc.ServerProxy("http://%s:%s"%(reducer1_host,reducer1_port),allow_none=True)
    proxy_reducer2= xc.ServerProxy("http://%s:%s"%(reducer2_host,reducer2_port),allow_none=True)
    proxy_reducer3= xc.ServerProxy("http://%s:%s"%(reducer3_host,reducer3_port),allow_none=True)

    if reduce_fn=='wordCountReducer':
        try:
            print("Running Word Count Reducer...")
            logging.debug("RPC Call to Word Count Reducer")
            res=proxy_reducer1.word_count_reducer("WORD_COUNT_REDUCE",datastore_host,datastore_port)
            print("reducer1:",res)
            res=proxy_reducer2.word_count_reducer("WORD_COUNT_REDUCE",datastore_host,datastore_port)
            print("reducer2:",res)
            res=proxy_reducer3.word_count_reducer("WORD_COUNT_REDUCE",datastore_host,datastore_port)
            print("reducer3:",res)
            return res
        except:
            logging.error("RPC Call to Word Count Reducer Failed")
    elif reduce_fn=='invertedIndexReducer':
        try:
            print("Running Inverted Index Reducer...")
            logging.debug("RPC Call to Inverted Index Reducer")
            res=proxy_reducer1.word_count_reducer("INVERTED_INDEX_REDUCE",datastore_host,datastore_port)
            print("reducer1:",res)
            res=proxy_reducer2.word_count_reducer("INVERTED_INDEX_REDUCE",datastore_host,datastore_port)
            print("reducer2:",res)
            res=proxy_reducer3.word_count_reducer("INVERTED_INDEX_REDUCE",datastore_host,datastore_port)
            print("reducer3:",res)
            return res
        except:
            logging.error("RPC all to Inverted Index Reducer Failed")
    else:
        print("Reduce Function Not Supported")
        logging.error("Reduce Function Not Supported")

#Function to write output onto the locaiton provided
def write_output(reduce_fn,output_location):
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    if reduce_fn=='wordCountReducer':
        req="get WORD_COUNT\r\n"
    elif reduce_fn=='invertedIndexReducer':
        req="get INVERTED_INDEX\r\n"


    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    ln=soc.recv(8)
    (length,)=unpack('>Q',ln)
    data=b''
    while len(data)< length:
        to_Read=length-len(data)
        data+=soc.recv(1024 if to_Read > 1024 else to_Read)

    input_data=data.decode()
    input_data = input_data.split("\n",2)[1]
    input_data=input_data.replace('(','').split(')')

    data1=''
    for i in input_data:
        data1+=i+"\n"
    if input_data:
        try:
            fd=open(output_location,'w+')
            if reduce_fn=='wordCountReducer':
                fd.write("WORD_COUNT:\n")
            elif reduce_fn=='invertedIndexReducer':
                fd.write("INVERTED_INDEX:\n")
            fd.write(data1)

        except:
            logging.error("Error writing data")

        print("Map-Reduce task completed. Please check the output location for results")
        logging.info("Map-Reduce successfully Completed")


# Function to Create Backup Datastore
def create_bsackup():

    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        soc.connect((datastore_host,datastore_port))
    except:
        logging.error("Datastore Connection Refused")

    req="backup\r\n"
    length=pack('>Q',len(req.encode()))
    soc.send(length)
    soc.send(req.encode())
    data=soc.recv(1400)
    logging.debug("Response from Datastore: %s"%data.decode())
    data=data.decode()
    data=data.strip()
    if(data=="BACKUP DONE"):
        return True
    else:
        return False


#Mapreduce interface
def run_mapred(input_data, map_fn, reduce_fn, output_location):

    project='adarsh-hegde'
    bucket='adarshs_storage_bucket_1'
    zone='us-central1-f'
    datastore_name='datastore-instance'
    wait=True

    compute = googleapiclient.discovery.build('compute', 'v1')
    datastore = get_instance(compute,project,zone,datastore_name)

    global datastore_host
    datastore_host= datastore['networkInterfaces'][0]['accessConfigs'][0]['natIP']

    map=shuffle=res=''
    print("Storing input data...")
    logging.info("Storing input data")
    store=store_input(map_fn,input_data)
    logging.debug("Response for input storage: %s",store)

    if store=='STORED':
        #**************************Mapper instance creation*******************************#
        print('Creating instance')
        logging.info("Creating Mapper 1")
        operation = create_instance(compute, project, zone, 'mapper-1', bucket, 'gs://adarshs_storage_bucket_1/mapper1-startup.sh')
        wait_for_operation(compute, project, zone, operation['name'])
        mapper_1 = get_instance(compute,project,zone,'mapper-1')
        global mapper1_host
        mapper1_host= mapper_1['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        logging.info("Mapper 1 Created")
        logging.info(mapper1_host)

        logging.info("Creating Mapper 2")
        operation = create_instance(compute, project, zone, 'mapper-2', bucket, 'gs://adarshs_storage_bucket_1/mapper2-startup.sh')
        wait_for_operation(compute, project, zone, operation['name'])
        mapper_2 = get_instance(compute,project,zone,'mapper-2')
        global mapper2_host
        mapper2_host= mapper_2['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        logging.info("Mapper 2 Created")
        logging.info(mapper2_host)

        logging.info("Creating Mapper 3")
        operation = create_instance(compute, project, zone, 'mapper-3', bucket, 'gs://adarshs_storage_bucket_1/mapper3-startup.sh')
        wait_for_operation(compute, project, zone, operation['name'])
        mapper_3 = get_instance(compute,project,zone,'mapper-3')
        global mapper3_host
        mapper3_host= mapper_3['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        logging.info("Mapper 3 Created")
        logging.info(mapper3_host)

        #**************************Mapper instance creation done*******************************#

        time.sleep(60)
        if mapper1_host and mapper2_host and mapper3_host:
            print("Starting Map Task")
            logging.info("Starting Map Task")
            map=invoke_map(map_fn,mapper1_host,mapper2_host,mapper3_host)
            logging.debug("Response from Map Task: %s",map)
            logging.info(map)
        else:
            logging.error("Mapper Creation Error")
            print("Error")

        if map=='MAP_DONE':
            print("Map Task Completed")
            logging.info("Map Task Completed")

            #Terminating mapper instances
            print("Terminating Mapper instances")
            logging.info("Terminating Mapper instances")
            instances = list_instances(compute, project, zone)
            for instance in instances:
                print(instance['name'])
                if(instance['name']=='mapper-1' or instance['name']=='mapper-2' or instance['name']=='mapper-3'):
                    operation = delete_instance(compute, project, zone, instance['name'])
                    wait_for_operation(compute, project, zone, operation['name'])
                    logging.info("Mapper %s Terminated!"%(instance['name']))

            print("Starting Shuffle Task")
            logging.info("Starting Shuffle Task")
            shuffle=invoke_shuffle(reduce_fn)
            logging.debug("Response from Shuffle Task: %s",shuffle)
        else:
            print("Map Task Failed!")
            logging.error("Map Task Failed!")

        if shuffle=='STORED':
            print("Shuffle Task Completed")
            logging.info("Shuffle Task Completed")

            #**************************Reducer instance creation*******************************#
            print('Creating instance')
            logging.info("Creating Reducer 1")
            operation = create_instance(compute, project, zone, 'reducer-1', bucket, 'gs://adarshs_storage_bucket_1/reducer1-startup.sh')
            wait_for_operation(compute, project, zone, operation['name'])
            reducer_1 = get_instance(compute,project,zone,'reducer-1')
            global reducer1_host
            reducer1_host= reducer_1['networkInterfaces'][0]['accessConfigs'][0]['natIP']
            logging.info("Reducer 1 Created")
            logging.info(reducer1_host)

            logging.info("Creating Reducer 2")
            operation = create_instance(compute, project, zone, 'reducer-2', bucket, 'gs://adarshs_storage_bucket_1/reducer2-startup.sh')
            wait_for_operation(compute, project, zone, operation['name'])
            reducer_2 = get_instance(compute,project,zone,'reducer-2')
            global reducer2_host
            reducer2_host= reducer_2['networkInterfaces'][0]['accessConfigs'][0]['natIP']
            logging.info("Reducer 2 Created")
            logging.info(reducer2_host)

            logging.info("Creating Reducer 3")
            operation = create_instance(compute, project, zone, 'reducer-3', bucket, 'gs://adarshs_storage_bucket_1/reducer3-startup.sh')
            wait_for_operation(compute, project, zone, operation['name'])
            reducer_3 = get_instance(compute,project,zone,'reducer-3')
            global reducer3_host
            reducer3_host= reducer_3['networkInterfaces'][0]['accessConfigs'][0]['natIP']
            logging.info("Reducer 3 Created")
            logging.info(reducer3_host)

            #**************************Reducer instance creation done*******************************#

            time.sleep(60)
            if reducer1_host and reducer2_host and reducer3_host:
                print("Starting Reduce Task")
                logging.info("Starting Reduce Task")
                res=invoke_reduce(reduce_fn,reducer1_host,reducer2_host,reducer3_host)
                logging.debug("Response from Reduce Task: %s",res)
            else:
                logging.error("Reducer Creation Error")
                print("Error")

            instances = list_instances(compute, project, zone)
            for instance in instances:
                print(instance['name'])
                if(instance['name']=='reducer-1' or instance['name']=='reducer-2' or instance['name']=='reducer-3'):
                    operation = delete_instance(compute, project, zone, instance['name'])
                    wait_for_operation(compute, project, zone, operation['name'])
                    logging.info("Redicer %s Terminated!"%(instance['name']))
        else:
            print("Shuffle Task Failed!")
            logging.error("Shuffle Task Failed!")

        if res=='REDUCER_DONE':
            write_output(reduce_fn,output_location)
            print("Creating Backup file...")
            logging.info("Creating Backup file...")
            bk=create_bsackup()
            if(bk):
                logging.debug("Backup Created")
                print("Backup File Created")
                return True
            else:
                logging.error("Backup error")
                print("Backup Creation Failed!")
                return False
        else:
            print("Reduce Rask Failed!")
            logging.error("Reduce Task Failed!")

            #Terminating residual instances in case of mapreduce failures
            instances = list_instances(compute, project, zone)
            for instance in instances:
                if(instance['name'] not in ['master-instance','datastore-instance']):
                    operation = delete_instance(compute, project, zone, instance['name'])
                    wait_for_operation(compute, project, zone, operation['name'])
                    logging.info("Instance %s Terminated!"%(instance['name']))
    else:
        print("Data not stored!")
        logging.error("Data not stored!")


logging.info("Master Spawned")

#run_mapred("http://www.gutenberg.org/files/3250/3250-h/3250-h.htm","wordCountMapper","wordCountReducer","home/adarshhegdegef/wordCount.txt")

#run_mapred(["http://www.gutenberg.org/files/23218/23218-h/23218-h.htm","http://www.gutenberg.org/files/19362/19362-h/19362-h.htm","http://www.gutenberg.org/files/21279/21279-h/21279-h.htm"],"invertedIndexMapper","invertedIndexReducer","home/adarshhegdegef/invertedIndex.txt")

#****************************************************End of Map Reduce Section***********************************************************#


class RequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                'REQUEST_METHOD': 'POST',
                'CONTENT_TYPE': self.headers['Content-Type'],
            }
        )
        out = io.TextIOWrapper(
            self.wfile,
            encoding='utf-8',
            line_buffering=False,
            write_through=True,
        )
        for field in form.keys():
            field_item = form[field]
            if field_item.filename:
                file_data = field_item.file.read()
                file_data=file_data.decode()
                json_data=json.loads(file_data)

                logging.info(json_data['input_data'])
                logging.info(json_data['map_fn'])
                logging.info(json_data['reduce_fn'])
                logging.info(json_data['output_location'])

                err=validate_config(json_data['input_data'],json_data['map_fn'],json_data['reduce_fn'],json_data['output_location'])
                logging.debug(err)
                if err:
                    self.send_response(500)
                    self.send_header('Content-Type',
                         'text/plain; charset=utf-8')
                    self.end_headers()
                    out.write(
                        'Error:{}\n'.format(err))
                    out.detach()
                else:
                    result=run_mapred(json_data['input_data'],json_data['map_fn'],json_data['reduce_fn'],json_data['output_location'])
                    res=''
                    try:
                        fd=open(json_data['output_location'],'r')
                        res=fd.read()
                        fd.close()
                    except:
                        logging.error("Output File not found")

                    if(result):
                        self.send_response(200)
                        self.send_header('Content-Type',
                             'text/plain; charset=utf-8')
                        self.end_headers()
                        out.write(
                            'MapReduce Output:{}'.format(res))
                        out.detach()
                    else:
                        self.send_response(500)
                        self.send_header('Content-Type',
                             'text/plain; charset=utf-8')
                        self.end_headers()
                        out.write(
                            'MapReduce Output: {}\n'.format(
                            "Failed"))
                        out.detach()

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 7001), RequestHandler)
    print('Master Started...')
    server.serve_forever()
