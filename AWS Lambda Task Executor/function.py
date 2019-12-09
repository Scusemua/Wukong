#import json
import ujson
import cloudpickle
from datetime import timedelta
import datetime
import base64 
import redis
import time
import dask
import dask.array
import dask.dataframe
import dask.bag
import pandas
import dask_ml 
import sklearn 
import math
import hashlib
from dask.core import istask
import uuid
import sys
import socket
import pickle 
import queue 
import tornado
from collections import defaultdict
from tornado import gen, netutil
from tornado.iostream import StreamClosedError, IOStream
from tornado.tcpclient import TCPClient
from tornado.gen import Return
from tornado.ioloop import IOLoop
import boto3
#from multiprocessing import Process, Pipe
from uhashring import HashRing 

from wukong_metrics import TaskExecutionBreakdown, LambdaExecutionBreakdown 
from utils import funcname
from exception import error_message
from serialization import Serialized, _extract_serialize, extract_serialize, to_frames, dumps, from_frames
from network import CommClosedError, FatalCommClosedError, parse_address, parse_host_port, connect_to_address, get_stream_address, unparse_host_port, TCP, connect_to_proxy

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.async_context import AsyncContext
xray_recorder.configure(service='my_service', sampling=True, context_missing='LOG_ERROR') #context=AsyncContext()
# This is the code of the AWS Lambda function. 
# Note that it requires the cloudpickle library.
# See the following link for documentation on
# how to deploy the function with cloudpickle included:
# https://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html#python-package-dependencies

no_value = "--no-value-sentinel--"
collection_types = (tuple, list, set, frozenset)

# The public IPv4 of the proxy.
proxy_address = 'ecX-X-XXX-XXX-XXX.us-east-2.compute.amazonaws.com'
# The endpoints (IP's and ports) of Redis instances to be used for 'large' objects.
# The form of this list should be [(IP1, port1), (IP2, port2), ..., (IPn, portn)]
big_redis_endpoints = []
# The endpoints (IP's and ports) of Redis instances to be used for 'small' objects.
# The form of this list should be [(IP1, port1), (IP2, port2), ..., (IPn, portn)]
small_redis_endpoints = []

big_nodes = dict()
small_nodes = dict()

first_small_client = None

hostnames_to_clients = dict()

# These are used as keys in dictionaries passed to and between Lambdas (i.e., Scheduler --> Lambda, Lambda --> Lambda, Lambda --> Proxy, Proxy --> Lambda, etc.)
starting_node_payload_key = "starts-at"
path_key_payload_key = "path-key"
previous_results_payload_key = "previous-results"
invoked_by_payload_key = "invoked-by"
executing_task_key = "executing-task"
executed_task_key = "executed-task"

#remote = ntp_client.request("north-america.pool.ntp.org")

for IP, port in big_redis_endpoints:
    redis_client = redis.StrictRedis(host=IP, port = port, db = 0)
    #big_redis_clients.append(redis_client)
    key_string = "node-" + str(IP) + ":" + str(port)
    host_name = key_string + ".FQDN"
    hostnames_to_clients[host_name] = redis_client 
    big_nodes[key_string] = {
        "hostname": host_name,
        "nodename": key_string,
        "instance": redis_client,
        "port": port,
        "vnodes": 200
    }

for IP, port in small_redis_endpoints:
    redis_client = redis.StrictRedis(host=IP, port = port, db = 0)
    if first_small_client is None:
       first_small_client = redis_client
    #small_redis_clients.append(redis_client)
    key_string = "node-" + str(IP) + ":" + str(port)
    host_name = key_string + ".FQDN"
    hostnames_to_clients[host_name] = redis_client 
    small_nodes[key_string] = {
        "hostname": host_name,
        "nodename": key_string,
        "instance": redis_client,
        "port": port,
        "vnodes": 200
    }

big_hash_ring = HashRing(big_nodes, hash_fn='ketama')
small_hash_ring = HashRing(small_nodes, hash_fn='ketama')

num_big_redis_instances = len(big_nodes)
num_small_redis_instances = len(small_nodes)
num_redis_instances = num_big_redis_instances + num_small_redis_instances

lambda_client = boto3.client('lambda')
 
if sys.version_info[0] == 2:
   unicode = unicode 
if sys.version_info[0] == 3:
   unicode = str 

def get_data_from_redis(key, key_to_hash = None, check_big_first = False):
   """ Retrieve the data stored at the given key from Redis. This automatically tries to get the key
       from the small Redis cluster and then the big Redis cluster, should the small cluster attempt fail.
       
       key (str) - The key at which the data is stored in Redis.
       key_to_hash (str) - The key we should give to the hash ring object for determining which redis shard has the data for 'key'.
                           We use this for path keys as the path key is "<task-key>---path". The value of 'key_to_hash' is the "<task-key>" portion
                           while the data is actually stored at the key "<task-key>---path".
       
       """
   if key_to_hash is None:
      key_to_hash = key
   if check_big_first:
      num_tries_outer = 1
      wait_time = 0.1
      while (num_tries_outer <= 3):
         # Attempt to grab the data from Redis. If it times out, then we'll try up to three times before giving up.
         try:
            result_big = big_hash_ring[key_to_hash].get(key)
            # If the result came back as None, then try the small cluster...
            if (result_big is None):
               num_tries_inner = 1
               num_tries_outer = 99999 # So we don't re-loop on the outer loop under any circumstance.
               while (num_tries_inner <= 3):
                  try:
                     result_small = small_hash_ring[key_to_hash].get(key)
                     # If the small result is also None, then something is wrong. The data should theoretically be stored somewhere. Raise an exception, print debug info.
                     if result_small is None:
                        print("[ERROR] Result from both big and small Redis clusters for key {} was none.".format(key))
                        print("[ERROR] Attempted to retrieve key from small instance {}:{} and big instance {}:{}".format(small_hash_ring.get_node_hostname(key), 
                                                                                                                  small_hash_ring.get_node_port(key),
                                                                                                                  big_hash_ring.get_node_hostname(key),
                                                                                                                  big_hash_ring.get_node_port(key)))
                        raise EnvironmentError("Failure to retrieve value at key {} from Redis".format(key))
                     else:
                        return result_small
                  except (ConnectionError, TimeoutError, Exception) as ex:
                     print("[ERROR] ", type(ex))
                     print(ex)
                     attempt_to_reconnect_to_small_instance(key_to_hash, num_tries_inner)
                     time.sleep(wait_time * num_tries_inner)
                     num_tries_inner += 1
            else:
               return result_big             
         except (ConnectionError, TimeoutError, Exception) as ex:
            print("[ERROR] ", type(ex))
            print(ex)
            attempt_to_reconnect_to_big_instance(key_to_hash, num_tries_outer)
            time.sleep(wait_time * num_tries_outer)  
            num_tries_outer += 1
   else:
      num_tries_outer = 1
      while (num_tries_outer <= 3):
         try:
            result_small = small_hash_ring[key_to_hash].get(key)
            # If the small result is None, then we'll try the big cluster.
            if (result_small is None):
               num_tries_inner = 1
               num_tries_outer = 99999 # So we don't re-loop on the outer loop under any circumstance.
               while (num_tries_inner <= 3):
                  try:
                     result_big = big_hash_ring[key_to_hash].get(key)
                     # If the big result is also None, then something is wrong. The data should've been stored somewhere. Raise exception and print debug info.
                     if result_big is None:
                        print("[ERROR] Result from both big and small Redis clusters for key {} was none.".format(key))
                        print("[ERROR] Attempted to retrieve key from small instance {}:{} and big instance {}:{}".format(small_hash_ring.get_node_hostname(key), 
                                                                                                                  small_hash_ring.get_node_port(key),
                                                                                                                  big_hash_ring.get_node_hostname(key),
                                                                                                                  big_hash_ring.get_node_port(key)))
                        raise EnvironmentError("Failure to retrieve value at key {} from Redis".format(key))
                     else:
                        return result_big               
                  except (ConnectionError, TimeoutError, Exception) as ex:
                     print("[ERROR] ", type(ex))
                     print(ex)
                     attempt_to_reconnect_to_big_instance(key_to_hash, num_tries_inner)
                     time.sleep(wait_time * num_tries_inner)
                     num_tries_inner += 1
            else:
               return result_small
         except (ConnectionError, TimeoutError, Exception) as ex:
            print("[ERROR] ", type(ex))
            print(ex)
            attempt_to_reconnect_to_small_instance(key_to_hash, num_tries_outer)
            time.sleep(wait_time * num_tries_outer)
            num_tries_outer += 1

def attempt_to_reconnect_to_big_instance(key, num_tries):
   hostname = big_hash_ring.get_node_hostname(key)[:-5].split(":")[0]
   port = big_hash_ring.get_node_port(key)
   print("[CONNECTION ERROR] Error when attempting to retrieve big data for task {} from big Redis instance at {}:{}... will try to reconnect (try {} of {})".format(key, hostname, port, num_tries, "3"))
   key_string = "node-" + hostname + ":" + str(port)
   redis_client = redis.StrictRedis(host=IP, port = port, db = 0)
   node_conf = {
      "hostname": host_name,
      "nodename": key_string,
      "instance": redis_client,
      "port": port,
      "vnodes": 40
   }      
   big_nodes[key_string] = node_conf
   hostnames_to_clients[host_name] = redis_client

   # Replace the node in the has ring with the new connection.
   big_hash_ring.add_node(key_string, node_conf)

def attempt_to_reconnect_to_small_instance(key, num_tries):
   # The host names have ".FQDN" appended to the end of them so we have to chop that part off. They also have the port attached so we remove that as well.
   hostname = small_hash_ring.get_node_hostname(key)[:-5].split(":")[0]
   port = small_hash_ring.get_node_port(key)
   print("[CONNECTION ERROR] Error when attempting to retrieve small data for task {} from small Redis instance at {}:{}... will try to reconnect (try {} of {})".format(key, hostname, port, num_tries, "3"))
   key_string = "node-" + hostname + ":" + str(port)
   redis_client = redis.StrictRedis(host=IP, port = port, db = 0)
   node_conf = {
      "hostname": host_name,
      "nodename": key_string,
      "instance": redis_client,
      "port": port,
      "vnodes": 40
   }      
   small_nodes[key_string] = node_conf
   hostnames_to_clients[host_name] = redis_client
   
   # The connection to the 'first_small_client' had timed out, so we need to reset this with the new connection object.
   if first_small_client.connection_pool.connection_kwargs["host"] == hostname and first_small_client.connection_pool.connection_kwargs["port"] == port:
      first_small_client = redis_client

   # Replace the node in the has ring with the new connection.
   small_hash_ring.add_node(key_string, node_conf)


def store_value_in_redis(key, value, threshold):
   """ Store a value in Redis. Handles checking which ring to use based on the given threshold."""
   num_tries = 1
   if sys.getsizeof(value) >= threshold:
      while num_tries <= 3:
         try:
            return big_hash_ring[key].set(key, value)
         except (ConnectionError, TimeoutError, Exception) as ex:
            print("[ERROR] ", type(ex))
            attempt_to_reconnect_to_big_instance(key, num_tries)
            num_tries += 1
         
   else:
      while num_tries <= 3:
         try:
            return small_hash_ring[key].set(key, value)
         except (ConnectionError, TimeoutError, Exception) as ex:
            print("[ERROR] ", type(ex))
            attempt_to_reconnect_to_small_instance(key, num_tries)
            num_tries += 1      


def check_and_store_value_in_redis(key, value, threshold, already_serialized = True):
   """ Store a value in Redis. Handles checking which ring to use based on the given threshold.
       Only stores the value if the given key does not already have a value stored."""

   print("[INFO] Checking-and-storing task {}'s value in Redis...".format(key))
   num_tries = 1
   exists = None
   if sys.getsizeof(value) >= threshold:
      while num_tries <= 3:
         try:
            # If we complete the call to .exists() but then lose connection, 
            # we can save the value locally so we don't have to re-execute the call to exists().             
            if exists is None:
               exists = big_hash_ring[key].exists(key)
            # If the value does not exist, then we store it.
            if exists == 0:
               if not already_serialized:
                  value = cloudpickle.dumps(value)
               big_hash_ring[key].set(key, value)
               return (True, value)
            else:
               return (False, None)
         except (ConnectionError, TimeoutError, Exception) as ex:
            print("[ERROR] ", type(ex))
            num_tries += 1
            attempt_to_reconnect_to_big_instance(key, num_tries)
   else:
      while num_tries <= 3:
         try:
            # If we complete the call to .exists() but then lose connection, 
            # we can save the value locally so we don't have to re-execute the call to exists(). 
            if exists is None:
               exists = small_hash_ring[key].exists(key)
            # If the value does not exist, then we store it.
            if exists == 0:
               if not already_serialized:
                  value = cloudpickle.dumps(value)         
               small_hash_ring[key].set(key, value)
               return (True, value)  
            else:
               return (False, None)
         except (ConnectionError, TimeoutError, Exception) as ex:
            print("[ERROR] ", type(ex))
            num_tries += 1        
            attempt_to_reconnect_to_small_instance(key, num_tries)              
   
   return (False, None)

def lambda_handler(event, context):
   """ This is the function directly executed by AWS Lambda. """
   return sequential(event, context, start_time = time.time())

@xray_recorder.capture("sequential")
def sequential(event, context, previous_results = None, start_time = time.time()):
   path_encoded = None 
   
   payload = None 

   # A wrapper around a bunch of diagnostic metrics.
   task_execution_breakdowns = dict() # TaskExecutionBreakdown()
   lambda_execution_breakdown = LambdaExecutionBreakdown(aws_request_id = context.aws_request_id,
                                                         start_time = start_time)
   # Check if we were sent the path directly. If not, then grab it from Redis.
   if path_key_payload_key in event:
      path_key = event[path_key_payload_key]
      print("[INIT] Obtained KEY {} in Lambda package. Payload must be obtained from Redis.".format(path_key))
      _start = time.time()
      #_path = hash_ring[path_key[:-7]].get(path_key)
      _path = get_data_from_redis(path_key, key_to_hash = path_key[:-7], check_big_first = True)
      _stop = time.time()
      redis_read_duration = _stop - _start 
      _size = sys.getsizeof(_path)
      lambda_execution_breakdown.add_read_time(path_key[:-7], _size, redis_read_duration, _start, _stop)
      lambda_execution_breakdown.redis_read_time += redis_read_duration
      payload = ujson.loads(_path.decode())
   else: 
       payload = event 
   nodes_map_serialized = payload["nodes-map"]
   
   # Check if the top-level payload (the one sent directly to the Lambda function) has a 'starts-at' key. If so,
   # then we should start there; some other function has decided that the node we should start on is 'starts-at'. 
   # The path may not start there, but the node will be contained within the path. Presumably everything before it
   # has already been executed (perhaps it was pulled down during a big task collapse).
   starting_node_key = payload["starting-node-key"]
   if starting_node_payload_key in event:
      print("The key {} was included in the payload to this Lambda function with the value {}".format(starting_node_payload_key, event[starting_node_payload_key]))
      starting_node_key = event[starting_node_payload_key]
   
   if invoked_by_payload_key in event:
      invoked_by_task = event[invoked_by_payload_key]
      print("This Lambda (which is first executing task {}) was invoked by task {}!".format(starting_node_key, invoked_by_task))
   else:
      print("This Lambda (which is first executing task {}) was invoked by the Scheduler!".format(starting_node_key))
   
   print("The initial Nodes Map (Serialized) contains all of the following task nodes: {}".format(str(list(nodes_map_serialized.keys()))))

   if previous_results is None:
       if previous_results_payload_key in event:
           previous_results = event[previous_results_payload_key]
           for key, value_encoded in previous_results.items():
              print("Decoding and deserializing directly-sent data {}".format(key))
              value_serialized = base64.b64decode(value_encoded)
              value = cloudpickle.loads(value_serialized)
              previous_results[key] = value
       else:
           previous_results = dict()
   
   process_path_start = time.time()
   result = process_path(nodes_map_serialized, 
                         starting_node_key, 
                         previous_results = previous_results, 
                         lambda_execution_breakdown = lambda_execution_breakdown, 
                         task_execution_breakdowns = task_execution_breakdowns)

   # Record the total time spent processing the current path. This is almost equivalent to the entire duration.
   lambda_execution_breakdown.process_path_time = time.time() - process_path_start
   
   # lengths = [cloudpickle.dumps(x) for x in task_execution_lengths]
   # redis_clients[0].lpush("timings", *task_execution_lengths)
   
   lambda_execution_breakdown.total_duration = time.time() - start_time
   if len(task_execution_breakdowns) > 0:
      #small_redis_clients[0].lpush("task_breakdowns", *[cloudpickle.dumps(breakdown) for breakdown in list(task_execution_breakdowns.values())])
      first_small_client.lpush("task_breakdowns", *[cloudpickle.dumps(breakdown) for breakdown in list(task_execution_breakdowns.values())])
   #small_redis_clients[0].lpush("lambda_durations", cloudpickle.dumps(lambda_execution_breakdown))
   first_small_client.lpush("lambda_durations", cloudpickle.dumps(lambda_execution_breakdown))

   # Store some diagnostics that we can see from the AWS X-Ray console.
   subsegment = xray_recorder.begin_subsegment("diagnostics")
   subsegment.put_annotation("tasks_executed", str(lambda_execution_breakdown.number_of_tasks_executed))
   subsegment.put_annotation("tasks_pulled_down", str(lambda_execution_breakdown.tasks_pulled_down))
   subsegment.put_annotation("total_redis_read_time", str(lambda_execution_breakdown.redis_read_time))
   subsegment.put_annotation("total_redis_write_time", str(lambda_execution_breakdown.redis_write_time))
   xray_recorder.end_subsegment()

   print("\n\n\nThis Lambda executed {} tasks in total.".format(lambda_execution_breakdown.number_of_tasks_executed))

   return result 

@xray_recorder.capture("process_path")
def process_path(nodes_map_serialized, starting_node_key, lambda_execution_breakdown, previous_results = dict(), task_execution_breakdowns = None):
   # Dictionary to store local results.
   # We map task keys to the result of the task.
   # results = {}
   
   # This is just like 'nodes_map_serialized' except we place deserialized nodes in here.
   nodes_map_deserialized = dict()

   subsegment = xray_recorder.begin_subsegment("deserializing_path")
    
   current_path_node_encoded = nodes_map_serialized[starting_node_key]
   current_path_node_serialized = base64.b64decode(current_path_node_encoded)
   current_path_node = cloudpickle.loads(current_path_node_serialized)

   xray_recorder.end_subsegment()

   # This flag indicates whether or not we should check the dependencies of the current task before executing it.
   # The dependencies are checked already for tasks that were invoked. If we are executing a 'become' task, however,
   # then the dependencies will not yet be checked. As a result, this flag is 'True' by default but will be set to False
   # once we start processing 'become't asks.
   deps_checked = True  
   
   # Process all of the tasks contained in the path.
   while current_path_node is not None:
      # Create a TaskExecutionBreakdown object for the current task.
      # We use this to keep track of a bunch of different metrics related to the current task.
      current_task_execution_breakdown = TaskExecutionBreakdown("TEMP_KEY")
      current_path_node.task_breakdown = current_task_execution_breakdown
      current_task_execution_breakdown.task_processing_start_time = time.time()
      lambda_execution_breakdown.number_of_tasks_executed += 1
      _start_for_current_task = time.time()
      if type(current_path_node.task_payload) is list:
         serialized_task_payload = current_path_node.task_payload
         frames = []
         for encoded in serialized_task_payload:
             frames.append(base64.b64decode(encoded))
         current_path_node.task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
      task_key = current_path_node.task_key
      
      # Update metrics related to the current task execution 
      current_task_execution_breakdown.deserialize_time = (time.time() - _start_for_current_task)
      current_task_execution_breakdown.task_key = task_key
      task_execution_breakdowns[current_task_execution_breakdown.task_key] = current_task_execution_breakdown
      
      # See the comment above for an explanation of 'deps_checked'
      if not deps_checked: 
         _dependency_checking_start = time.time()
         print("[PROCESSING] Processing Task ", task_key, ". Checking dependencies first.")
         subsegment = xray_recorder.begin_subsegment("checking dependencies of begin task")
         key_counter = str(task_key) + "---dep-counter"

         # Check to make sure all of the dependencies are resolved before continuing onto execution.
         # Since we're just incrementing the dependency counter, we use the small hash ring.
         redis_client = small_hash_ring.get_node_instance(task_key)
         _redis_start = time.time()
         dependencies_completed = redis_client.incr(key_counter)
         _redis_stop = time.time()

         # Increment the total redis read time metric.
         _duration = (_redis_stop - _redis_start)
         lambda_execution_breakdown.redis_read_time += _duration
         current_task_execution_breakdown.redis_read_time += _duration
         lambda_execution_breakdown.add_read_time(key_counter, sys.getsizeof(dependencies_completed), _duration, _redis_start, _redis_stop)
         xray_recorder.end_subsegment()

         # Decode the result.
         dependencies_completed = dependencies_completed.decode() 
         if dependencies_completed != len(current_path_node.task_payload["dependencies"]):
            print("[WARNING] Cannot execute 'become' task {} as only {}/{} dependencies are finished and available.".format(current_path_node.task_payload["key"], dependencies_completed, len(current_path_node.task_payload["dependencies"])))
            return   
         current_task_execution_breakdown.dependency_checking = (time.time() - _dependency_checking_start)
      else:
         print("[PROCESSING] Processing Task ", task_key, ". Dependencies have already been checked.")

      _start = time.time()
      # Process the task via the process_task method. Pass in the 
      # local results variable for the "previous_results" keyword argument.
      result = process_task(current_path_node.task_payload, task_key, previous_results, lambda_debug = current_path_node.lambda_debug, lambda_execution_breakdown = lambda_execution_breakdown, current_task_execution_breakdown = current_task_execution_breakdown)
      lambda_execution_breakdown.process_task_time += (time.time() - _start)
      
      # If the task erred, just return... 
      if result["op"] == "task-erred":
         print("[ERROR] Execution of task {} resulted in an error.".format(task_key))
         print("Result: ", result)
         print("Result['exception']: ", result["exception"])
         return {
            'statusCode': 400,
            'body': ujson.dumps(result["exception"])
         }   
      print("[EXECUTION] Result of task {} computed in {} seconds.".format(task_key, result["execution-length"]))
      value = result["result"]
      # Create the previous_results dictionary if it does not exist.
      if previous_results is None:
          previous_results = dict()

      _start = time.time()
      
      nodes_to_process = queue.Queue()
      current_path_node_unprocessed = UnprocessedNode(current_path_node, result)
      nodes_to_process.put_nowait(current_path_node_unprocessed)
      become_node = None 
      become_path = None 
      need_to_download_become_path = False
      
      # Iterate over each node, processing its out edges.
      while not nodes_to_process.empty():
         node_processing = nodes_to_process.get_nowait()
         if (hasattr(node_processing.path_node, "task_breakdown") and node_processing.path_node.task_breakdown != None):
            print("Unprocessed Node {} had a Task Breakdown associated with it.".format(node_processing.path_node.task_key))
            current_task_execution_breakdown = node_processing.path_node.task_breakdown
         else:
            print("Unprocessed Node {} did NOT have a Task Breakdown associated with it. Creating one now...".format(node_processing.path_node.task_key))
            current_task_execution_breakdown = TaskExecutionBreakdown(node_processing.path_node.task_key)
            _start_for_current_task = time.time()
            current_task_execution_breakdown.task_processing_start_time = _start_for_current_task
            node_processing.path_node.task_breakdown = current_task_execution_breakdown
         # Check if we need to deserialize the task payload...
         if type(node_processing.path_node.task_payload) is list:
            serialized_task_payload = node_processing.path_node.task_payload
            frames = []
            for encoded in serialized_task_payload:
               frames.append(base64.b64decode(encoded))
            node_processing.path_node.task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
         print("Processing node with task key: ", node_processing.path_node.task_key)
         print("node_processing.path_node: ", node_processing.path_node.__str__())
         out_edges = list()
         
         # TO-DO:
         # (1) CHECK IF NODES ARE IN nodes_map_deserialized
         # (2) IF NOT, CHECK IF NODES ARE IN nodes_map_serialized
         # (3) IF NOT, DOWNLOAD RELEVANT NODES/PATHS FROM REDIS USING 'starts_at' FIELD ON PATH NODE.
         # (3) IF NOT, DOWNLOAD RELEVANT NODES/PATHS FROM REDIS USING 'starts_at' FIELD ON PATH NODE.

         # We will likely need to download these nodes/their paths...
         for invoke_key in node_processing.path_node.get_downstream_tasks():
            # Check DESERIALIZED nodes.
            if invoke_key in nodes_map_deserialized:
               out_edges.append(nodes_map_deserialized[invoke_key])
            # Check SERIALIZED nodes.
            elif invoke_key in nodes_map_serialized:
               _invoke_node_encoded = nodes_map_serialized[invoke_key]
               _invoke_node_node_serialized = base64.b64decode(_invoke_node_encoded)
               _invoke_node = cloudpickle.loads(_invoke_node_node_serialized)
               nodes_map_deserialized[invoke_key] = _invoke_node
               out_edges.append(_invoke_node)
            # Retrieve from Redis.
            else:
               key_at_path_start = node_processing.path_node.starts_at 
               path_key = key_at_path_start + "---path"
               _start = time.time()
               _path_encoded = get_data_from_redis(path_key, key_to_hash = path_key[:-7], check_big_first = True)
               _stop = time.time()
               redis_read_duration = _stop - _start 
               _size = sys.getsizeof(_path_encoded)
               lambda_execution_breakdown.add_read_time(path_key[:-7], _size, redis_read_duration, _start, _stop)
               lambda_execution_breakdown.redis_read_time += redis_read_duration
               path_payload = ujson.loads(_path_encoded.decode())

               # Create a reference to the serialized nodes map.
               new_nodes_map_serialized = path_payload["nodes-map"]

               # Add the new serialized nodes to the local 'nodes_map_serialized'.
               nodes_map_serialized.update(new_nodes_map_serialized)

               # Decode, deserialize, and store the node in nodes_map_deserialized.
               _invoke_node_encoded = nodes_map_serialized[invoke_key]
               _invoke_node_node_serialized = base64.b64decode(_invoke_node_encoded)
               _invoke_node = cloudpickle.loads(_invoke_node_node_serialized)
               nodes_map_deserialized[invoke_key] = _invoke_node
               out_edges.append(_invoke_node)
         
         # If 'node_processing' has out-edges that require processing, then process them.
         if len(out_edges) > 0:
            next_nodes, become_candidate, need_to_download_become_path = process_out_edges(out_edges,
                                                                           node_processing.path_node.task_key,
                                                                           node_processing.path_node,
                                                                           node_processing.result,
                                                                           node_processing.path_node.task_payload,
                                                                           nodes_map_serialized,
                                                                           nodes_map_deserialized,
                                                                           previous_results,
                                                                           needs_become = (become_node == None),
                                                                           lambda_execution_breakdown = lambda_execution_breakdown,
                                                                           current_task_execution_breakdown = current_task_execution_breakdown,
                                                                           task_execution_breakdowns = task_execution_breakdowns)
            # Add the new nodes to 'nodes_to_process'
            for new_node_to_process in next_nodes:
               nodes_to_process.put_nowait(new_node_to_process)
            current_task_execution_breakdown.total_time_spent_on_this_task = (time.time() - _start_for_current_task)
            current_task_execution_breakdown.task_processing_end_time = time.time()
            task_execution_breakdowns[current_task_execution_breakdown.task_key] = current_task_execution_breakdown
            # If we do not have a value for our 'become' node yet and a 
            # value was returned, then use that value for our 'become 'node.
            if (become_node is None and become_candidate is not None):
               print("The current 'become_node' is None and we have a 'become_candidate' in that of {}. We're gonna use it.".format(become_candidate.task_key))
               become_node = become_candidate
               if need_to_download_become_path:
                  path_key = become_node.task_key + "---path"
                  _start = time.time()
                  #_path = hash_ring[path_key[:-7]].get(path_key)
                  _path = get_data_from_redis(path_key, key_to_hash = path_key[:-7], check_big_first = True)
                  _stop = time.time()
                  redis_read_duration = _stop - _start 
                  _size = sys.getsizeof(_path)
                  lambda_execution_breakdown.add_read_time(path_key[:-7], _size, redis_read_duration, _start, _stop)
                  lambda_execution_breakdown.redis_read_time += redis_read_duration
                  become_path = ujson.loads(_path.decode())
      
      # The loop "while not nodes_to_process.empty():" ends here.
      
      # If we were supposed to download the path, then we need to deserialize the becomes node.
      if become_node is not None and need_to_download_become_path:
         next_nodes_map_serialized = become_path["nodes-map"]
         # Add all of the new serialized nodes to the next_nodes_map_serialized dictionary.
         nodes_map_serialized.update(next_nodes_map_serialized)
         next_become_node_encoded = nodes_map_serialized[become_node.task_key]
         next_become_node_serialized = base64.b64decode(next_become_node_encoded)
         become_node = cloudpickle.loads(next_become_node_serialized)

      _now = time.time()
      current_task_execution_breakdown.total_time_spent_on_this_task = (_now - _start_for_current_task)
      current_task_execution_breakdown.task_processing_end_time = _now
      task_execution_breakdowns[current_task_execution_breakdown.task_key] = current_task_execution_breakdown
      if become_node is not None: 
         print("[BECOMING] Task {} is going to become task {}.".format(current_path_node.task_key, become_node.task_key))
         current_path_node = become_node
         deps_checked = True 
      else:
         # For now, I am just using the "dask-workers-1" channel to publish messages to the Scheduler.
         # As long as I am creating the Redis-polling processes on the Scheduler, we can arbitrarily use whatever channel we want.
         # We only send the message if there are no downstream tasks bc otherwise this isn't a root node.
         if current_path_node.num_downstream_tasks() == 0:
            print("[PROCESSING] - Task {} does not have any downstream tasks. It's the last task on its path.".format(current_path_node.task_key))
            _write_start = time.time()
            _serialized_value = cloudpickle.dumps(value)
            # By convention, store final results in the big node cluster.
            big_hash_ring[task_key].set(task_key, _serialized_value)
            _write_stop = time.time()
            _write_duration = _write_stop - _write_start
            current_task_execution_breakdown.redis_write_time += _write_duration
            lambda_execution_breakdown.redis_write_time += _write_duration
            lambda_execution_breakdown.add_write_time(task_key, sys.getsizeof(_serialized_value), _write_duration, _write_start, _write_stop)
            print("[MESSAGING] - Publishing 'lambda-result' message to Redis Channel: dask-workers-1")
            print("Final result: ", str(value))
            payload = dict(
               op="lambda-result",
               lambda_id = context.aws_request_id,
               task_key = current_path_node.task_key,
               execution_length = result["execution-length"],
               time_sent = datetime.datetime.utcnow().isoformat()
            )
            # By convention, use the small Redis clients for message passing.
            #small_redis_clients[0].publish("dask-workers-1", ujson.dumps(payload))
            first_small_client.publish("dask-workers-1", ujson.dumps(payload))
         else:
            print("[PROCESSING] - There are no available downstream tasks which depend upon task {}.".format(current_path_node.task_key))
            print("Become for {}: {}".format(current_path_node.task_key, current_path_node.become))
            print("Invoke for {}: {}".format(current_path_node.task_key, current_path_node.invoke))            
            _redis_start = time.time()
            # Make sure we write the data to Redis. First check if it is there. If it isn't, write it.
            # We don't want to waste time serializing the value unless we're actually going to store it.
            stored, value_serialized = check_and_store_value_in_redis(task_key, value, current_path_node.task_payload["storage_threshold"], already_serialized = False)  
            _redis_stop = time.time()
            if (stored):
               print("Data for task {} has been written to Redis.".format(task_key))
               _write_duration = _redis_stop - _redis_start 
               current_task_execution_breakdown.redis_write_time += _write_duration 
               lambda_execution_breakdown.redis_write_time += _write_duration 
               lambda_execution_breakdown.add_write_time(task_key, sys.getsizeof(value_serialized), _write_duration, _redis_start, _redis_stop) 
            else:
               # If we didn't write anything, then we at least called .exist() to see if it exists.
               _read_duration = _redis_stop - _redis_start 
               current_task_execution_breakdown.redis_read_time += _read_duration 
               lambda_execution_breakdown.redis_read_time += _read_duration 
               lambda_execution_breakdown.add_read_time(task_key, sys.getsizeof(value_serialized), _read_duration, _redis_start, _redis_stop)                
         current_path_node = None 
      xray_recorder.end_subsegment()
   return {
      'statusCode': 202,
      'body': ujson.dumps("Success")  
   }

@xray_recorder.capture("check-if-ready-for-execution")
def is_task_ready_for_execution(task_key, num_dependencies, increment = False, task_node = None):
   """ Check if the given task (specified by 'task_key') is ready for execution based on whether 
       or not all of its dependencies have completed. We check by comparing its dependency counter
       (which is an integer stored in Redis) against the number of dependencies it has, specified by
       'num_dependencies'. In some instances, we want to check how many of its dependencies have
       resolved without also incrementing the counter. Whether or not we increment is determined by
       the value of the given 'increment' flag. Finally, the task_node parameter just enables us to
       print the list of dependencies for a given task (specifically, we print the task keys)."""
   num_tries = 1
   while (num_tries <= 3):
      try:
         key_counter = str(task_key) + "---dep-counter"
         #associated_redis_client = hash_ring.get_node_instance(task_key)
         associated_redis_client = small_hash_ring.get_node_instance(task_key)
         print("Checking dependencies for task {}. Increment: {}".format(task_key, increment))
         if increment:
            dependencies_completed = associated_redis_client.incr(key_counter)
            if dependencies_completed == num_dependencies:
               print("Task {} IS ready for execution. All {} dependencies completed.".format(task_key, num_dependencies))
               return True 
            else:
               print("Task {} is NOT ready for execution. Only {} of {} dependencies have been completed.".format(task_key, dependencies_completed, num_dependencies))
               if (task_node != None):
                  print("Dependencies for {}: {}".format(task_key, task_node.task_payload["dependencies"]))
               return False 
         else:
            dependencies_completed = int(associated_redis_client.get(key_counter).decode())
            if dependencies_completed == (num_dependencies - 1):
               print("Task {} IS ready for execution. All {} dependencies completed.".format(task_key, num_dependencies))
               return True 
            else:
               print("Task {} is NOT ready for execution. Only {} of {} dependencies have been completed.".format(task_key, dependencies_completed, num_dependencies))
               if (task_node != None):
                  print("Dependencies for {}: {}".format(task_key, task_node.task_payload["dependencies"]))         
               return False       
      except (ConnectionError, TimeoutError, Exception) as ex:
         print("[ERROR] ", type(ex))
         # print("[ERROR] Connection exception when checking if task {} is ready for execution...".format(task_key))
         attempt_to_reconnect_to_small_instance(task_key, 1)
         time.sleep(0.1 * num_tries)   
         num_tries = num_tries + 1      
    

@xray_recorder.capture("process-out-edges")
def process_out_edges(out_edges, 
                      current_task_key, 
                      current_path_node, 
                      result, 
                      task_payload, 
                      nodes_map_serialized, 
                      nodes_map_deserialized,
                      previous_results, 
                      needs_become = True,
                      lambda_execution_breakdown = None, 
                      current_task_execution_breakdown = None,
                      task_execution_breakdowns = dict()):
   value = result.pop('result', None)

   # Cache the result locally for future task executions.
   # In some cases, we may have already cached the results (i.e. if we previously pulled down and executed locally the current task).
   # In these situations, 'value' would currently be None, and we'd overwrite the existing entry in 'previous_results' if we
   # did not perform this check first.
   if value is not None:
      previous_results[current_task_key] = value
   
   if value is None:
      print("[ERROR] Previous result value is none for task {}".format(current_task_key))

   # Will be set to True or False if necessary, otherwise this will just be ignored.
   need_to_download_become_path = False # Default value; doesn't mean anything really.
   
   # Serialize the resulting value.
   subsegment = xray_recorder.begin_subsegment("serializing-value")
   value_serialized = cloudpickle.dumps(value)
   xray_recorder.end_subsegment()
   size = sys.getsizeof(value_serialized)

   # We temporarily use chunk_task_threshold to determine if a task is big enough to execute its downstream tasks locally.
   execute_local_threshold = task_payload["chunk-task-threshold"]   
   next_nodes_for_processing = list()
   ready_to_invoke = list()
   current_data_written = False 
   current_become_node = None 
   become_is_ready = (not needs_become) # If 'needs_become' is True, then the become node is certainly not ready.
                                        # However, if 'needs_become' is False (i.e., we already have a 'become' node),
                                        # then the 'become' node IS ready, so we just initialize this to true.

   print("execute_local_threshold: ", execute_local_threshold)
   print("Size of serialized data for {}: {} bytes.".format(current_path_node.task_key, size))
   if size >= execute_local_threshold:
      next_nodes_for_processing = process_big(current_path_node, 
                                   value_serialized,
                                   nodes_map_serialized,
                                   nodes_map_deserialized,
                                   previous_results,
                                   out_edges_to_process = out_edges, 
                                   current_data_written = current_data_written,
                                   threshold = execute_local_threshold,
                                   lambda_execution_breakdown = lambda_execution_breakdown,
                                   current_task_execution_breakdown = current_task_execution_breakdown,
                                   task_execution_breakdowns = task_execution_breakdowns)
   else:
      if needs_become:
         # We do check to see if the type is list first though. If it is a list,
         # then that means it is still in its serialized form. If it isn't a list,
         # then we must've deseralized it earlier.
         if current_path_node.become in nodes_map_deserialized:
            current_become_node = nodes_map_deserialized[current_path_node.become]
            # Deserialize the payload so we can check its dependencies and all.
            if type(current_become_node.task_payload) is list:
               serialized_task_payload = current_become_node.task_payload
               frames = []
               for encoded in serialized_task_payload:
                  frames.append(base64.b64decode(encoded))
               current_become_node.task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
         else:
            current_become_node_encoded = nodes_map_serialized[current_path_node.become]
            current_become_node_serialized = base64.b64decode(current_become_node_encoded)
            current_become_node = cloudpickle.loads(current_become_node_serialized)
            
            # Deserialize the payload so we can check its dependencies and all.
            if type(current_become_node.task_payload) is list:
               serialized_task_payload = current_become_node.task_payload
               frames = []
               for encoded in serialized_task_payload:
                  frames.append(base64.b64decode(encoded))
               current_become_node.task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
            nodes_map_deserialized[current_path_node.become] = current_become_node
         _dep_check_start = time.time()
         print("Checking if the pre-assigned become node {} (for task {}) is ready to execute. Not incrementing...".format(current_path_node.task_key, current_become_node.task_key))
         become_is_ready = is_task_ready_for_execution(current_become_node.task_key, len(current_become_node.task_payload["dependencies"]), task_node = current_become_node, increment = False)
         _dep_check_dur = time.time() - _dep_check_start
         current_task_execution_breakdown.dependency_checking += _dep_check_dur
         if become_is_ready:
            print("Task {} is ready for execution and can be used as a become node for task {}! Removing it from out-edges before process_small()...".format(current_become_node.task_key, current_path_node.task_key))
            # If become_is_ready is true at this point, then it means 
            # the 'become' node is the one that was originally assigned 
            # and we therefore do not need to download anything.
            need_to_download_become_path = False
         
            # Increment the dependency counter for book-keeping purposes. Since the node was already ready, 
            # there will be no other tasks/nodes that see this task is done and attempt to execute it.
            # is_task_ready_for_execution(current_become_node.task_key, len(current_become_node.task_payload["dependencies"]), task_node = current_become_node, increment = True)

            # needs_become = False 

            # We're going to 'become' this node; we do not want to process (and invoke) it. 
            # Thus, we remove it from the out_edges list.
            out_edges.remove(current_become_node)

      _start_processing = time.time()   
      ready_to_invoke, current_become_node, need_to_download_become_path, current_data_written = process_small(current_path_node, 
                                                                                                               become_is_ready, 
                                                                                                               current_become_node, 
                                                                                                               out_edges,
                                                                                                               needs_become,
                                                                                                               value_serialized,
                                                                                                               nodes_map_serialized,
                                                                                                               nodes_map_deserialized,
                                                                                                               lambda_execution_breakdown = lambda_execution_breakdown,
                                                                                                               current_task_execution_breakdown = current_task_execution_breakdown,
                                                                                                               task_execution_breakdowns = task_execution_breakdowns)
      _dur_processing = time.time() - _start_processing
      lambda_execution_breakdown.process_downstream_tasks_time += _dur_processing              
      current_task_execution_breakdown.process_downstream_tasks_time += _dur_processing                                                                                        
   
   # Invoke everything that is ready to be invoked.
   for node_that_can_execute in ready_to_invoke:
      # We're going to attempt to pack some data into this Lambda function invocation so that it doesn't need to go to Redis for the data.
      data_for_invocation = dict()
      # size in bytes of the dependencies we're sending 
      total_size = 0 
      total_size += sys.getsizeof(node_that_can_execute.starts_at + "---path") # Account for the size of the path key...
      total_size += sys.getsizeof(node_that_can_execute.task_key)              # Account for the size of the first node that should be executed...
      total_size += sys.getsizeof(current_task_key)                            # Account for 'invoked by' field (for debugging)
      max_size = 256000
      for dep in node_that_can_execute.task_payload["dependencies"]:
         print("Checking if we have {} locally, and if so, can we send it to the new Lambda directly?".format(dep))
         if dep in previous_results:
            val = previous_results[dep]
            val_serialized = cloudpickle.dumps(val)
            val_encoded = base64.encodestring(val_serialized).decode('utf-8')
            val_size = sys.getsizeof(val_encoded)
            print("The value for {} is {} bytes after serialization and encoding.".format(dep, val_size))
            # If the size of the serialized-and-encoded value is greater than 256kB, then just skip it.
            if (val_size > max_size):
               continue
            size_remaining = max_size - total_size
            
            # If we have space remaining for the value, then we'll use it.
            if (val_size < size_remaining):
               print("We had {} bytes remaining, and since {} is {} bytes, we can send it directly.".format(size_remaining, dep, val_size))
               data_for_invocation[dep] = val_encoded
      payload = {path_key_payload_key: node_that_can_execute.starts_at + "---path", 
                 previous_results_payload_key: data_for_invocation, 
                 starting_node_payload_key: node_that_can_execute.task_key,
                 invoked_by_payload_key: current_task_key}

      # Invoke the downstream task.
      print("[INVOCATION] - Invoking downstream task {}.".format(node_that_can_execute.task_key))
      _start_invoke = time.time()
      lambda_client.invoke(FunctionName="WukongExecutor", InvocationType='Event', Payload=ujson.dumps(payload))
      _invoke_duration = time.time() - _start_invoke 
      lambda_execution_breakdown.invoking_downstream_tasks += _invoke_duration
      current_task_execution_breakdown.invoking_downstream_tasks += _invoke_duration

   print("Returning from 'process_out_edges()'")
   print("next_nodes_for_processing: ", next_nodes_for_processing)
   print("current_become_node: ", current_become_node.__str__())
   print("need_to_download_become_path: ", need_to_download_become_path)
   return next_nodes_for_processing, current_become_node, need_to_download_become_path

@xray_recorder.capture("process_downstream_tasks_for_small_output")
def process_small(current_path_node, 
                  become_is_ready, 
                  current_become_node, 
                  out_edges_to_process, 
                  needs_become, 
                  value_serialized,
                  nodes_map_serialized, 
                  nodes_map_deserialized,
                  lambda_execution_breakdown = None, 
                  current_task_execution_breakdown = None,
                  task_execution_breakdowns = dict()):
   # If we're supposed to use the proxy to parallelize the invocation of downstream tasks, then do so...
   if current_path_node.use_proxy == True:
      print("Task {} is going to use the proxy...".format(current_path_node.task_key))

      # Some X-Ray diagnostics.
      subsegment = xray_recorder.begin_subsegment("store-redis-proxy")
      payload_for_proxy = ujson.dumps({"op": "set", 
                                       "task-key": current_path_node.task_key, 
                                       "value-encoded": base64.encodestring(value_serialized).decode('utf-8')
                                       "lambda_id": context.aws_request_id})
      size_of_payload = sys.getsizeof(payload_for_proxy)
      subsegment.put_annotation("size_payload_for_proxy", str(size_of_payload))

      # Grab the channel on which we shall publish the message.
      redis_proxy_channel = current_path_node.task_payload["proxy-channel"]
      _start = time.time()
      
      # Public the message.
      # small_redis_clients[0].publish(redis_proxy_channel, payload_for_proxy)
      first_small_client.publish(redis_proxy_channel, payload_for_proxy)

      # Commented this out as I'm not sure if it counts as writing to Redis or not... 
      #lambda_execution_breakdown.redis_write_time += (time.time() - _start)
      #current_task_execution_breakdown.redis_write_time += (time.time() - _start)
      xray_recorder.end_subsegment()

      if needs_become:
         if type(current_become_node.task_payload) is list:
            # Deserialize the payload so we can check its dependencies and all.
            serialized_task_payload = current_become_node.task_payload
            frames = []
            for encoded in serialized_task_payload:
               frames.append(base64.b64decode(encoded))
            current_become_node.task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
         print("Processing pre-assigned become node {}...".format(current_become_node.task_key))

         # Check if the downstream task (out-edge) is ready to execute...
         _processing_downstream_task_start = time.time()
         ready_to_execute = is_task_ready_for_execution(current_become_node.task_key, len(current_become_node.task_payload["dependencies"]), task_node = current_become_node, increment = True)
         _processing_downstream_task_duration = time.time() - _processing_downstream_task_start
         current_task_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration
         lambda_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration

         # Since we're using the proxy, we just attempt to use the pre-assigned 'become' node. We check it here; if it's not ready, then we'll return 'None.'
         # The variables we're turning are: ready_to_invoke, current_become_node, need_to_download_become_path, current_data_written
         if ready_to_execute:
            return [], current_become_node, False, False
      return [], None, False, False
   
   # Not using the proxy...
   ready_to_invoke = list()
   print("Processing SMALL for current task {}".format(current_path_node.task_key))
   #associated_redis_hostname = hash_ring.get_node_hostname(current_path_node.task_key)
   print("Writing resulting data from execution of task {} to Redis".format(current_path_node.task_key))

   # Store the result in redis.
   subsegment = xray_recorder.begin_subsegment("store-redis-direct")
   subsegment.put_annotation("size_payload_redis_direct", str(sys.getsizeof(value_serialized)))
   _start = time.time()
   #success = hash_ring[current_path_node.task_key].set(current_path_node.task_key, value_serialized)
   store_value_in_redis(current_path_node.task_key, value_serialized, threshold = current_path_node.task_payload["storage_threshold"])
   _stop = time.time()
   _redis_write_duration = _stop - _start
   lambda_execution_breakdown.add_write_time(current_path_node.task_key, sys.getsizeof(value_serialized), _redis_write_duration, _start, _stop)
   lambda_execution_breakdown.redis_write_time += _redis_write_duration
   current_task_execution_breakdown.redis_write_time += _redis_write_duration
   current_data_written = True 
   need_to_download_become_path = False # Default value is false bc if we enter this function with a become node already selected and ready to execute,
                                        # then that become node is going to be the pre-assigned one, meaning we won't need to download anything. If instead
                                        # we need to find a become node during this function's execution, we'll set this value to true if necessary. So it
                                        # should be false to begin with since either (1) we have a become node already, and we have all its data or (2) we
                                        # won't have any become node at all and thus we have nothing we'd need to download anyway.

   for out_edge_node in out_edges_to_process:
      # We do check to see if the type is list first though. If it is a list,
      # then that means it is still in its serialized form. If it isn't a list,
      # then we must've deseralized it earlier.
      if type(out_edge_node.task_payload) is list:
         # Deserialize the payload so we can check its dependencies and all.
         serialized_task_payload = out_edge_node.task_payload
         frames = []
         for encoded in serialized_task_payload:
            frames.append(base64.b64decode(encoded))
         out_edge_node.task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
      print("Processing out_edge {}...".format(out_edge_node.task_key))

      # Check if the downstream task (out-edge) is ready to execute...
      _processing_downstream_task_start = time.time()
      ready_to_execute = is_task_ready_for_execution(out_edge_node.task_key, len(out_edge_node.task_payload["dependencies"]), task_node = out_edge_node, increment = True)
      _processing_downstream_task_duration = time.time() - _processing_downstream_task_start
      current_task_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration
      lambda_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration
      if ready_to_execute:
         # If we don't have a 'become' node yet, then
         # we'll use this node instead of invoking it. 
         if needs_become is True and become_is_ready is False:
            print("Since task {} was ready for execution AND we needed a 'become' node, we're going to use it!".format(out_edge_node.task_key))
            current_become_node = out_edge_node 
            become_is_ready = True 
            needs_become = False 
            
            # If we end up re-assigning our original 'become' 
            # node as our final, official 'become' node, then 
            # we actually have the path data and do not need 
            # to download it. Otherwise we will need to download 
            # the path data, so set the flag accordingly.
            if current_become_node.task_key == current_path_node.become:
               need_to_download_become_path = False 
            else:
               need_to_download_become_path = True 
         else: #need to make sure we don't try to invoke the 'become' node
            ready_to_invoke.append(out_edge_node)
      else:
         # It isn't ready. Ignore it -- move on.
         pass

   if become_is_ready:
      print("Returning from process_small() with ready become node: ")
      print("ready_to_invoke: ", ready_to_invoke)
      print("current_become_node: ", current_become_node.__str__())
      print("need_to_download_become_path: ", need_to_download_become_path, ", current_data_written: ", current_data_written)
      return ready_to_invoke, current_become_node, need_to_download_become_path, current_data_written
   else:
      print("Returning from process_small() without ready become node: ")
      print("ready_to_invoke: ", ready_to_invoke)
      print("current_become_node: ", "None")
      print("need_to_download_become_path: ", "N/A", ", current_data_written: ", current_data_written)
      return ready_to_invoke, None, need_to_download_become_path, current_data_written

@xray_recorder.capture("process_downstream_tasks_for_BIG_output")
def process_big(current_path_node, 
                value_serialized, 
                nodes_map_serialized, 
                nodes_map_deserialized,
                previous_results,
                out_edges_to_process = list(), 
                current_data_written = False, 
                threshold = 50000000, 
                lambda_execution_breakdown = None, 
                current_task_execution_breakdown = None,
                task_execution_breakdowns = dict()):
   tasks_pulled_down = list()
   next_nodes_for_processing = []

   print("Processing BIG for current task {}".format(current_path_node.task_key))
   for out_edge in out_edges_to_process:
      # We do check to see if the type is list first though. If it is a list,
      # then that means it is still in its serialized form. If it isn't a list,
      # then we must've deseralized it earlier.
      if type(out_edge.task_payload) == list:
         # Deserialize the payload so we can check its dependencies and all.
         serialized_task_payload = out_edge.task_payload
         frames = []
         for encoded in serialized_task_payload:
            frames.append(base64.b64decode(encoded))
         out_edge.task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
      print("Processing out_edge {}...".format(out_edge.task_key))

      # Check if the downstream task (out-edge) is ready to execute again...
      _processing_downstream_task_start = time.time()
      ready_to_execute = is_task_ready_for_execution(out_edge.task_key, len(out_edge.task_payload["dependencies"]), task_node = out_edge, increment = False)
      _processing_downstream_task_duration = time.time() - _processing_downstream_task_start
      current_task_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration
      lambda_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration     

      # If it is ready to execute, then pull it down for local execution. 
      if ready_to_execute:
         tasks_pulled_down.append(out_edge)
         
         # Increment the dependecy counter associated with this task for book-keeping purposes, though no other task should look at it.
         # This will not result in the out_edge possibly being executed twice, as in order for 'ready_to_execute' to be True, the value 
         # of the dependency counter for out_edge had to have been equal to (out_edge.NUM_DEPENDENCIES - 1), meaning every other task
         # which might try to invoke/execute it has finished.
         key_counter = str(out_edge.task_key) + "---dep-counter"
         small_hash_ring.get_node_instance(out_edge.task_key).incr(key_counter)
      else:
         # If we haven't already written our data to Redis yet,
         # then first write the data, then increment the dependency
         # counter. Make sure to do one last check to see if the task
         # is ready to execute, though.
         if current_data_written is False:
            # Store the result in redis.
            subsegment = xray_recorder.begin_subsegment("store-redis-direct")
            subsegment.put_annotation("size_payload_redis_direct", str(sys.getsizeof(value_serialized)))
            _start = time.time()
            #associated_redis_hostname = hash_ring.get_node_hostname(current_path_node.task_key)
            print("Writing data for big task {} to Redis".format(current_path_node.task_key))
            #success = hash_ring[current_path_node.task_key].set(current_path_node.task_key, value_serialized)
            store_value_in_redis(current_path_node.task_key, value_serialized, current_path_node.task_payload["storage_threshold"])
            _stop = time.time()
            _redis_write_duration = _stop - _start
            lambda_execution_breakdown.add_write_time(current_path_node.task_key, sys.getsizeof(value_serialized), _redis_write_duration, _start, _stop)
            lambda_execution_breakdown.redis_write_time += _redis_write_duration
            current_task_execution_breakdown.redis_write_time += _redis_write_duration            
            current_data_written = True 

            # Check if the downstream task (out-edge) is ready to execute again...
            _processing_downstream_task_start = time.time()
            ready_to_execute = is_task_ready_for_execution(out_edge.task_key, len(out_edge.task_payload["dependencies"]), task_node = out_edge, increment = True)
            _processing_downstream_task_duration = time.time() - _processing_downstream_task_start
            current_task_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration
            lambda_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration  

            # If it's now ready, then that means the other tasks finished in the time we were writing our data. We need to execute the task locally then.      
            if ready_to_execute:
               tasks_pulled_down.append(out_edge)
         else:
            # We have to increment now. We may as well check again just in case it has since finished. (We _need_ to check again, since if it has since 
            # finished, we are technically last as far as the Wukong is concerned, and thus we need to execute the task.)
            _processing_downstream_task_start = time.time()
            ready_to_execute = is_task_ready_for_execution(out_edge.task_key, len(out_edge.task_payload["dependencies"]), task_node = out_edge, increment = True)
            _processing_downstream_task_duration = time.time() - _processing_downstream_task_start
            current_task_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration
            lambda_execution_breakdown.process_downstream_tasks_time += _processing_downstream_task_duration 

            # If it's ready now, then pull it down for local execution. The other tasks finished executing between when we last checked and now, so we need
            # to execute this task locally now, or else no other tasks will execute it. 
            if ready_to_execute:
               tasks_pulled_down.append(out_edge)

   for task_node in tasks_pulled_down:
      print("Executing task {} locally...".format(task_node.task_key))
      pulled_down_task_breakdown = TaskExecutionBreakdown(task_node.task_key)
      pulled_down_task_breakdown.task_processing_start_time = time.time()
      task_node.task_breakdown = pulled_down_task_breakdown

      task_execution_breakdowns[task_node.task_key] = pulled_down_task_breakdown

      # Execute the task locally.
      result = process_task(task_node.task_payload, 
                            task_node.task_key, 
                            previous_results, 
                            lambda_debug = current_path_node.lambda_debug,
                            lambda_execution_breakdown = lambda_execution_breakdown, 
                            current_task_execution_breakdown = pulled_down_task_breakdown)
      # print("Result of local execution of {}: {}".format(task_node.task_key, result.__str__()))
      print("Finished local execution of task {}".format(task_node.task_key))
      value = None
      if "result" in result:
         value = result["result"]
      previous_results[task_node.task_key] = value
      print("Number of downstream tasks for {} (which we just executed locally): {}".format(task_node.task_key, task_node.num_downstream_tasks()))

      lambda_execution_breakdown.number_of_tasks_executed += 1
      lambda_execution_breakdown.tasks_pulled_down += 1
      # If we just processed a root node, then we need to send a message to the Scheduler via PubSub.
      if task_node.num_downstream_tasks() == 0:
         # Serialize the resulting value.
         subsegment = xray_recorder.begin_subsegment("serializing-value")
         _value_serialized = cloudpickle.dumps(value)
         xray_recorder.end_subsegment()
         _size = sys.getsizeof(_value_serialized)

         print("[PROCESSING] - There are no downstream tasks which depend upon task {}.".format(task_node.task_key))
         print("Become for {}: {}".format(task_node.task_key, task_node.become))
         print("Invoke for {}: {}".format(task_node.task_key, task_node.invoke))
         print("Final result: ", str(value))
         print("[MESSAGING] - Publishing 'lambda-result' message to Redis Channel: dask-workers-1")
         payload = dict(
            op="lambda-result",
            task_key=task_node.task_key,
            execution_length=result["execution-length"],
            lambda_id = context.aws_request_id
         )

         _start_writing_to_redis = time.time()
         stored, _ = check_and_store_value_in_redis(task_node.task_key, _value_serialized, task_node.task_payload["storage_threshold"], already_serialized = True)
         _done_writing_to_redis = time.time()
         # If we stored the data, then print a message indicating that we did. That also means we should add the execution time of the method
         # invocation (for check_and_store_value_in_redis) to the "redis_write_time" variables of the current task breakdown and lambda breakdown.
         if stored:
            print("Data for task {} has been written to Redis. Now sending message.".format(task_node.task_key))
            _write_to_redis_duration = _done_writing_to_redis - _start_writing_to_redis
            lambda_execution_breakdown.redis_write_time += _start_writing_to_redis
            pulled_down_task_breakdown.redis_write_time += _start_writing_to_redis

         #if hash_ring[task_node.task_key].exists(task_node.task_key) == 0:
         #   print("Data for task {} has not yet been written to Redis. Doing so now (before message is published).".format(task_node.task_key))
         #   _start_write = time.time()
         #   hash_ring[task_node.task_key].set(task_node.task_key, _value_serialized)
         #   _redis_write_duration = time.time() - _start_write
         #   lambda_execution_breakdown.redis_write_time += _redis_write_duration
         #   pulled_down_task_breakdown.redis_write_time += _redis_write_duration             
         # For now, I am just using the "dask-workers-1" channel to publish messages to the Scheduler.
         # As long as I am creating the Redis-polling processes on the Scheduler, we can arbitrarily use whatever channel we want.
         #small_redis_clients[0].publish("dask-workers-1", ujson.dumps(payload)) 
         first_small_client.publish("dask-workers-1", ujson.dumps(payload)) 
      else:
         unprocessed = UnprocessedNode(task_node, result)
         next_nodes_for_processing.append(unprocessed)
   return next_nodes_for_processing

@xray_recorder.capture("process_task")
def process_task(task_definition, task_key, previous_results, lambda_debug = False, lambda_execution_breakdown = None, current_task_execution_breakdown = None):
   """ This function is responsible for executing the current task.
       
       It is responsible for grabbing and processing the dependencies for the current task."""
   # Grab the key associated with this task and use it to store the task's result in Elasticache.
   key = task_key or task_definition['key']   
   print("[EXECUTION] Executing task {} .".format(key))
   args_serialized = None 
   func_serialized = None
   kwargs_serialized = None
   if "function" in task_definition:
      func_serialized = task_definition["function"]
   if "args" in task_definition:
      args_serialized = task_definition["args"]
   task_serialized = no_value
   if "task" in task_definition:
      task_serialized = task_definition["task"]
   if "kwargs" in task_definition:
      kwargs_serialized = task_definition["kwargs"]
   func, args, kwargs = _deserialize(func_serialized, args_serialized, kwargs_serialized, task_serialized)

   # This is a list of dependencies in the form of string keys (which are keys to the Elasticache server)
   subsegment = xray_recorder.begin_subsegment("getting-dependencies-from-redis")
   dependencies = task_definition["dependencies"]
   data = {}
   print("[PREP] {} dependencies required.".format(len(dependencies)))
   print("Dependencies Required: ", dependencies)

   responses = dict()
   
   # Initialize this variable as we're going to use it shortly.
   time_spent_retrieving_dependencies_from_redis = 0

   # We were using transactions/mget for this, but most tasks do not have a large number of dependencies
   # so mget() probably wasn't helping much. Additionally, the keys are hashed evenly across all of the 
   # shards so they'll mostly be spread out evenly, further decreasing the effectiveness of mget().
   for dep in dependencies:
      if dep in previous_results:
         continue
      print("Retrieving dependency {} from Redis...".format(dep))
      _start = time.time()
      #val = hash_ring[dep].get(dep)
      val = get_data_from_redis(dep)
      _stop = time.time()
      _duration = _stop - _start
      lambda_execution_breakdown.add_read_time(dep, sys.getsizeof(val), _duration, _start, _stop)
      # Increment the aggregate total of the dependency retrievals.
      time_spent_retrieving_dependencies_from_redis += _duration
      #if val is None:
      #   addr = hash_ring.get_node_hostname(dep)
      #   print("[ {} ] [ERROR] Dependency {} is None... Failed to retrieve dependency from Redis Client at {}".format(datetime.datetime.utcnow(), dep, addr))
      responses[dep] = val
    
   # And update the Redis read metric.
   lambda_execution_breakdown.redis_read_time += time_spent_retrieving_dependencies_from_redis
   current_task_execution_breakdown.redis_read_time += time_spent_retrieving_dependencies_from_redis

   _process_deps_start = time.time()

   xray_recorder.end_subsegment()
   subsegment = xray_recorder.begin_subsegment("processing-dependencies")
   subsegment.put_annotation("num_dependencies", str(len(dependencies)))

   # Add the previous results to the data dictionary.
   print("[PREP] Appending previous_results to current data list.")
   printable_prev_res = list()
   for previous_result_key, previous_result_value in previous_results.items():
      if previous_result_value is not None:
         printable_prev_res.append(previous_result_key + ": Non-None value")
      else:
         printable_prev_res.append(previous_result_key + ": None")
   print("Contents of previous_results: ", str(printable_prev_res))
   data.update(previous_results)
   size_of_deps = 0

   # Keep track of any chunked data that we need to de-chunk.
   chunked_data = dict()
   
   for task_key, serialized_dependency_data in responses.items():
      size_of_deps = size_of_deps + sys.getsizeof(serialized_dependency_data)
      # TO-DO: Handle missing dependencies?
      # Make multiple attempts to retrieve missing dependency from Redis in case we've been timing out.
      if serialized_dependency_data is None:
         print("[ {} ] [ERROR] Dependency {} is None... Failed to retrieve dependency {} from Redis. Exiting.".format(datetime.datetime.utcnow(), task_key, task_key))
         return {
            "op": "task-erred",   
            "statusCode": 400,
            "exception": "dependency {} is None".format(task_key),
            "body": "dependency {} is None".format(task_key)
         } 
      deserialized_data = cloudpickle.loads(serialized_dependency_data)
      
      data[task_key] = deserialized_data
      # Keep a local record of the value obtained from Redis.
      previous_results[task_key] = deserialized_data
   subsegment.put_annotation("size_of_deps", str(size_of_deps))
   xray_recorder.end_subsegment()
   subsegment = xray_recorder.begin_subsegment("pack_data")
   args2 = pack_data(args, data, key_types=(bytes, unicode))
   kwargs2 = pack_data(kwargs, data, key_types=(bytes, unicode))
   
   # Update dependency processing metric. 
   current_task_execution_breakdown.dependency_processing = (time.time() - _process_deps_start)
   function_start_time = time.time()
   xray_recorder.end_subsegment()

   args_str = str(args)
   kwargs_str = str(kwargs)
   if len(args_str) > 50:
      args_str = args_str[0:50] + "..."
   if len(kwargs_str) > 50:
      kwargs_str = kwargs_str[0:50] + "..."
   print("Applying function {} with arguments {}, kwargs {} (key is {})".format(func, args_str, kwargs_str, key))
   
   # If Lambda Debugging is enabled, send a message to the Scheduler indicating that we're now executing the task.
   if lambda_debug:
      payload = {
         "op": executing_task_key,
         "task_key": key,
         "lambda_id": context.aws_request_id
      }
      first_small_client.publish("dask-workers-1", ujson.dumps(payload))

   result = apply_function(func, args2, kwargs2, key)
   function_end_time = time.time()
   execution_length = function_end_time - function_start_time  
   result["execution-length"] = execution_length
   result["start-time"] = function_start_time
   result["end_time"] = function_end_time

   # If Lambda Debugging is enabled, send a message to the Scheduler indicating that we're now executing the task.
   if lambda_debug:
      payload = {
         "op": executed_task_key,
         "task_key": key,
         "start_time": function_start_time,
         "end_time": function_end_time,
         "execution_time": execution_length,
         "lambda_id": context.aws_request_id
      }
      first_small_client.publish("dask-workers-1", ujson.dumps(payload))
   
   # Update execution-related metrics.
   current_task_execution_breakdown.task_execution_start_time = function_start_time
   current_task_execution_breakdown.task_execution_end_time = function_end_time
   current_task_execution_breakdown.task_execution = execution_length
   return result

@xray_recorder.capture_async("deserialize_payload")
@gen.coroutine
def deserialize_payload(payload):
   msg = yield from_frames(payload)
   raise gen.Return(msg)
   
@xray_recorder.capture_async("send_message_to_scheduler")
@gen.coroutine
def send_message_to_scheduler(msg, scheduler_address):
   #connection_args = {'ssl_context': None, 'require_encryption': None}
   connection_args = {}
   comm = yield connect_to_address(scheduler_address, connection_args = connection_args)
   comm.name = "Lambda->Scheduler"
   #comm._server = weakref.ref(self)
   print("Sending msg ", msg, " to scheduler")
   yield comm.write(msg, serializers=["msgpack"])   
   #yield comm.close()

@xray_recorder.capture("_deserialize")
def _deserialize(function=None, args=None, kwargs=None, task=no_value):
   """ Deserialize task inputs and regularize to func, args, kwargs """
   if function is not None:
      function = pickle.loads(function)
   if args:
      args = pickle.loads(args)
   if kwargs:
      kwargs = pickle.loads(kwargs)

   if task is not no_value:
      assert not function and not args and not kwargs
      function = execute_task
      args = (task,)

   return function, args or (), kwargs or {}

@xray_recorder.capture("execute_task")
def execute_task(task):
   """ Evaluate a nested task

   >>> inc = lambda x: x + 1
   >>> execute_task((inc, 1))
   2
   >>> execute_task((sum, [1, 2, (inc, 3)]))
   7
   """
   if istask(task):
      func, args = task[0], task[1:]
      return func(*map(execute_task, args))
   elif isinstance(task, list):
      return list(map(execute_task, task))
   else:
      return task

@xray_recorder.capture("apply_function")
def apply_function(function, args, kwargs, key):
   """ Run a function, collect information

   Returns
   -------
   msg: dictionary with status, result/error, timings, etc..
   """
   try:
      result = function(*args, **kwargs)
   except Exception as e:
      print("Exception e: ", e)
      msg = error_message(e)
      msg["op"] = "task-erred"
      msg["actual-exception"] = e
   else:
      msg = {
         "op": "task-finished",
         "status": "OK",
         "result": result,
         "key": key
         #"nbytes": sizeof(result),
         #"type": type(result) if result is not None else None,
     }
   return msg  

def pack_data(o, d, key_types=object):
   """ Merge known data into tuple or dict

   Parameters
   ----------
   o:
      core data structures containing literals and keys
   d: dict
      mapping of keys to data

   Examples
   --------
   >>> data = {'x': 1}
   >>> pack_data(('x', 'y'), data)
   (1, 'y')
   >>> pack_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
   {'a': 1, 'b': 'y'}
   >>> pack_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
   {'a': [1], 'b': 'y'}
   """
   typ = type(o)
   try:
      if isinstance(o, key_types) and o in d:
         return d[o]
   except TypeError:
      pass

   if typ in collection_types:
      return typ([pack_data(x, d, key_types=key_types) for x in o])
   elif typ is dict:
      return {k: pack_data(v, d, key_types=key_types) for k, v in o.items()}
   else:
      return o

class Reschedule(Exception):
   """ Reschedule this task

   Raising this exception will stop the current execution of the task and ask
   the scheduler to reschedule this task, possibly on a different machine.

   This does not guarantee that the task will move onto a different machine.
   The scheduler will proceed through its normal heuristics to determine the
   optimal machine to accept this task.  The machine will likely change if the
   load across the cluster has significantly changed since first scheduling
   the task.
   """

   pass     
   
def get_msg_safe_str(msg):
   """ Make a worker msg, which contains args and kwargs, safe to cast to str:
   allowing for some arguments to raise exceptions during conversion and
   ignoring them.
   """

   class Repr(object):
      def __init__(self, f, val):
         self._f = f
         self._val = val

      def __repr__(self):
         return self._f(self._val)

   msg = msg.copy()
   if "args" in msg:
      msg["args"] = Repr(convert_args_to_str, msg["args"])
   if "kwargs" in msg:
      msg["kwargs"] = Repr(convert_kwargs_to_str, msg["kwargs"])
   return msg

def convert_args_to_str(args, max_len=None):
   """ Convert args to a string, allowing for some arguments to raise
   exceptions during conversion and ignoring them.
   """
   length = 0
   strs = ["" for i in range(len(args))]
   for i, arg in enumerate(args):
      try:
         sarg = repr(arg)
      except Exception:
         sarg = "< could not convert arg to str >"
      strs[i] = sarg
      length += len(sarg) + 2
      if max_len is not None and length > max_len:
         return "({}".format(", ".join(strs[: i + 1]))[:max_len]
   else:
      return "({})".format(", ".join(strs))

def convert_kwargs_to_str(kwargs, max_len=None):
   """ Convert kwargs to a string, allowing for some arguments to raise
   exceptions during conversion and ignoring them.
   """
   length = 0
   strs = ["" for i in range(len(kwargs))]
   for i, (argname, arg) in enumerate(kwargs.items()):
      try:
         sarg = repr(arg)
      except Exception:
         sarg = "< could not convert arg to str >"
      skwarg = repr(argname) + ": " + sarg
      strs[i] = skwarg
      length += len(skwarg) + 2
      if max_len is not None and length > max_len:
         return "{{{}".format(", ".join(strs[: i + 1]))[:max_len]
   else:
      return "{{{}}}".format(", ".join(strs))
   
#@xray_recorder.capture("def store_value_in_redis(value, key)")
#def store_value_in_redis(key, value):
#    print("[REDIS PROCESS] Storing value {} in Redis at key {}.".format(value, key))
#    value_serialized = cloudpickle.dumps(value)
#    get_redis_client(key).set(key, value)
#    print("[REDIS PROCESS] Value stored successfully.")

class UnprocessedNode(object):
   def __init__(self, path_node, result):
      self.path_node = path_node
      self.result = result

   def __eq__(self, value):
      """Overrides the default implementation"""
      if isinstance(value, UnprocessedNode):
         return self.path_node.__eq__(value.path_node)
      return False