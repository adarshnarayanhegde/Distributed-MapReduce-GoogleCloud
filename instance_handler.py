#Instance handling codes in this file are referred from google cloud documents

import googleapiclient
from google.cloud import storage
import argparse
import os
import time
import googleapiclient.discovery
from six.moves import input
from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def create_bucket(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.create_bucket(bucket_name)
    print('Bucket {} created'.format(bucket.name))


#create_bucket('adarshs_storage_bucket_1')

def delete_bucket(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()
    print('Bucket {} deleted'.format(bucket.name))


def list_buckets():
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        print(bucket.name)

list_buckets()


# [START list_instances]
def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None
# [END list_instances]


# [START create_instance]
def create_instance(compute, project, zone, name, bucket,startup_script):
    # Get the latest Debian Jessie image.
    image_response = compute.images().getFromFamily(
        project='debian-cloud', family='debian-9').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    '''startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'startup-script.sh'), 'r').read()'''
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

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
            },
                {
                'key': 'url',
                'value': image_url
            }, {
                'key': 'text',
                'value': image_caption
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


# [START delete_instance]
def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
# [END delete_instance]


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


def creater(project, bucket, zone, instance_name, start_up, wait=True):

    compute = googleapiclient.discovery.build('compute', 'v1')
    print('Creating instance.')

    operation = create_instance(compute, project, zone, instance_name, bucket,start_up)
    wait_for_operation(compute, project, zone, operation['name'])

    instances = list_instances(compute, project, zone)
    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])


def starter(project, bucket, zone, instance_name, start_up, wait=True):
    compute = googleapiclient.discovery.build('compute', 'v1')
    print("Starting instance")
    operation = start_instance(compute,project,zone,instance_name)
    wait_for_operation(compute, project, zone, operation['name'])


def stopper(project, bucket, zone, instance_name, start_up, wait=True):
    compute = googleapiclient.discovery.build('compute', 'v1')
    instances = list_instances(compute, project, zone)
    print("Stopping instances")
    for instance in instances:
        print(instance['name'])
        operation = stop_instance(compute,project,zone,instance['name'])
        wait_for_operation(compute, project, zone, operation['name'])

def terminator(project, bucket, zone, instance_name, start_up, wait=True):
    compute = googleapiclient.discovery.build('compute', 'v1')
    print('Deleting instance.')

    operation = delete_instance(compute, project, zone, instance_name)
    wait_for_operation(compute, project, zone, operation['name'])


def getter(project, zone, instance):
    compute = googleapiclient.discovery.build('compute', 'v1')
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = project
    zone = zone
    instance = instance

    request = service.instances().get(project=project, zone=zone, instance=instance)
    response = request.execute()
    #pprint(response)

    return compute.instances().get(
        project=project,
        zone=zone,
        instance=instance).execute()

while True:
    instance=int(input("\n1.Datastore\n2.Master\n3.Exit\nPlease enter your choice:"))
    if instance==1:
        print("Datastore:")
        store=int(input("\n1.Create\n2.Start\n3.Stop\n4.Delete\n5.Get\n6.Exit\nPlease enter your choice:"))
        if store==1:
            creater('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'datastore-instance','gs://adarshs_storage_bucket_1/datastore-startup.sh')
        elif store==2:
            starter('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'datastore-instance','gs://adarshs_storage_bucket_1/datastore-startup.sh')
        elif store==3:
            stopper('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'datastore-instance','gs://adarshs_storage_bucket_1/datastore-startup.sh')
        elif store==4:
            terminator('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'datastore-instance','gs://adarshs_storage_bucket_1/datastore-startup.sh')
        elif store==5:
            res=getter('adarsh-hegde', 'us-central1-f', 'datastore-instance')
            natIP= res['networkInterfaces'][0]['accessConfigs'][0]['natIP']
            print("natIP:",natIP)
        else:
            break

    elif instance==2:
        print("Master:")
        store=int(input("\n1.Create\n2.Start\n3.Stop\n4.Delete\n5.Get\n6. Exit\nPlease enter your choice:"))
        if store==1:
            creater('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'master-instance','gs://adarshs_storage_bucket_1/master-startup.sh')
        elif store==2:
            starter('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'master-instance','gs://adarshs_storage_bucket_1/master-startup.sh')
        elif store==3:
            stopper('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'master-instance','gs://adarshs_storage_bucket_1/master-startup.sh')
        elif store==4:
            terminator('adarsh-hegde', 'adarshs_storage_bucket_1', 'us-central1-f', 'master-instance','gs://adarshs_storage_bucket_1/master-startup.sh')
        elif store==5:
            res=getter('adarsh-hegde', 'us-central1-f', 'master-instance')
            natIP= res['networkInterfaces'][0]['accessConfigs'][0]['natIP']
            print("natIP:",natIP)
        else:
            break
    else:
        break
