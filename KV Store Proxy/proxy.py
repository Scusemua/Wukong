import sys
import traceback
sys.path.append('/usr/local/lib/python3.7/site-packages')
sys.path.append('/usr/local/lib64/python3.7/site-packages')

from pathing import Path, PathNode 
import aioredis
import asyncio
import redis # For the polling processes 

import hashlib
import argparse 

import math

import errno
import socket
import struct

from collections import defaultdict
import pickle
import cloudpickle 
import json
import multiprocessing
import base64
import boto3 
import time
import datetime 

import multiprocessing
from multiprocessing import Pipe, Process
import queue 
from uhashring import HashRing 

from serialization import Serialized, _extract_serialize, extract_serialize, to_frames, dumps, from_frames
from network import CommClosedError, FatalCommClosedError, parse_address, parse_host_port, connect_to_address, get_stream_address, unparse_host_port, TCP, connect_to_proxy
from proxy_lambda_invoker import ProxyLambdaInvoker 

from tornado.ioloop import IOLoop
from tornado.ioloop import PeriodicCallback
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
from tornado.netutil import bind_sockets
import tornado 

define('proxy_port', default=8989, help="TCP port to use for the proxy itself")
define('encoding', default='utf-8', help="String encoding")
define('redis_port1', default=6379, help="Port for the Redis cluster")
define('redis_port2', default=6380, help="Port for the Redis cluster")
define('redis_endpoint', default="ec2-3-89-228-222.compute-1.amazonaws.com", help="IP Address of the Redis cluster")

# TO-DO:
# - Synchronize message handling / processing. There could be race conditions where 'set' gets called
#   while still processing the "graph-init" operation, meaning downstream tasks may not be locally available yet.
#   Need to address this possibility.

class CommClosedError(IOError):
    pass

class RedisProxy(object):
    """Tornado asycnrhonous TCP server co-located with a Redis cluster."""

    def __init__(self, redis_endpoints, lambda_client):
        self.redis_endpoints = redis_endpoints
        self.lambda_client = lambda_client
        self.completed_tasks = set()

        self.scheduler_address = ""                 # The address of the modified Dask Distributed scheduler 
        self.serialized_paths = {}                  # List of serialized paths retrieved from Redis 
        self.path_nodes = {}                        # Mapping of task keys to path nodes 
        self.serialized_path_nodes = {}             # List of serialized path nodes.
        self.handlers = {
                "set": self.handle_set,
                "graph-init": self.handle_graph_init,
                "start": self.handle_start
            }
        
    def task_completed(task_key):
        task_dependencies_key = task_key + "--deps"

    def start(self):
        self.server = TCPServer()
        self.server.handle_stream = self.handle_stream
        self.server.listen(options.proxy_port)
        self.port = options.proxy_port
        self.need_to_process = []
        #print("Redis proxy listening at {}:{}".format(self.redis_endpoint, self.port))
        print("Redis proxy listening on port {}".format(self.port))
        
        self.loop = IOLoop.current()

        # On the next iteration of the IOLoop, we will attempt to connect to the Redis servers.
        self.loop.add_callback(self.connect_to_redis_servers)

        # Start the IOLoop! 
        self.loop.start()

    @gen.coroutine
    def connect_to_redis_servers(self):
        print("[ {} ] Connecting to Redis servers...".format(datetime.datetime.utcnow()))
        self.redis_clients = []
        self.redis_nodes = dict()
        counter = 1 

        for IP, port in self.redis_endpoints:
            print("[ {} ] Attempting to connect to Redis server {}/{} at {}:{}".format(datetime.datetime.utcnow(), counter, len(self.redis_endpoints), IP, port))
            connection = yield aioredis.create_connection(address = (IP, port), loop = IOLoop.current().asyncio_loop, encoding = options.encoding) #redis.StrictRedis(host=IP, port = port, db = 0)
            redis_client = aioredis.Redis(connection)
            print("[ {} ] Successfully connected to Redis server {}.".format(datetime.datetime.utcnow(), counter))
            self.redis_clients.append(redis_client)
            key_string = "node-" + str(IP) + ":" + str(port)
            self.redis_nodes[key_string] = {
                "hostname": key_string + ".FQDN",
                "nodename": key_string,
                "instance": redis_client,
                "port": port,
                "vnodes": 40
            }
            counter += 1

        self.hash_ring = HashRing(self.redis_nodes)
        #for port in self.redis_ports:
        #    print("[ {} ] Attempting to connect to Redis server {}/{} at {}:{}".format(datetime.datetime.utcnow(), counter, len(self.redis_ports), self.redis_endpoint, port))
        #    #connection = IOLoop.current().run_sync(lambda: aioredis.create_connection(address = (self.redis_endpoint, port), loop = IOLoop.current().asyncio_loop, encoding = options.encoding))
        #    connection = yield aioredis.create_connection(address = (self.redis_endpoint, port), loop = IOLoop.current().asyncio_loop, encoding = options.encoding)
        #    redis_client = aioredis.Redis(connection)
        #    self.redis_clients.append(redis_client)
        #    counter = counter + 1 
        self.num_redis_clients = len(self.redis_clients)

    @gen.coroutine
    def process_task(self, task_key, _value_encoded = None, message = None, print_debug = False):      
        #print("[ {} ] Processing task {} now...".format(datetime.datetime.utcnow(), task_key))
        
        # Decode the value but keep it serialized.              
        value_encoded = _value_encoded or message["value-encoded"]
        value_serialized = base64.b64decode(value_encoded)
        task_node = self.path_nodes[task_key]  
        task_payload = task_node.task_payload

        # Store the result in redis.
        #yield self.get_redis_client(task_key).set(task_key, value_serialized)
        
        redis_client = self.hash_ring.get_node_instance(task_key) #[task_key].set(task_key, value_serialized)
        print("[ {} ] Storing task {} in Redis client listening at {}".format(datetime.datetime.utcnow(), task_key, redis_client.address))
        yield redis_client.set(task_key, value_serialized)

        self.completed_tasks.add(task_key)

        num_dependencies_of_dependents = task_payload["num-dependencies-of-dependents"]
        can_now_execute = []

        print("[ {} ] Value for {} successfully stored in Redis. Checking dependencies/invoke nodes now...".format(datetime.datetime.utcnow(), task_key))

        #if print_debug:
        #    print("[ {} ] [PROCESSING] Now processing the downstream tasks for task {}".format(datetime.datetime.utcnow(), task_key))

        for invoke_key in task_node.invoke:
            futures = []
            invoke_node = self.path_nodes[invoke_key]

            # Grab the key of the "invoke" node and check how many dependencies it has.
            # Next, check Redis to see how many dependencies are available. If all dependencies
            # are available, then we append this node to the can_now_execute list. Otherwise, we skip it.
            # The invoke node will be executed eventually once its final dependency resolves.
            dependency_counter_key = invoke_key + "---dep-counter"
            num_dependencies = num_dependencies_of_dependents[invoke_key]

            # We create a pipeline for incrementing the counter and getting its value (atomically). 
            # We do this to reduce the number of round trips required for this step of the process.
            #redis_pipeline = self.get_redis_client(invoke_key).pipeline()
            redis_pipeline = self.hash_ring.get_node_instance(invoke_key).pipeline()

            futures.append(redis_pipeline.incr(dependency_counter_key))  # Enqueue the atomic increment operation.
            futures.append(redis_pipeline.get(dependency_counter_key))   # Enqueue the atomic get operation AFTER the increment operation.

            # Execute the pipeline; save the responses in a local variable.
            result = yield redis_pipeline.execute()
            responses = yield asyncio.gather(*futures)

            # Decode and cast the result of the 'get' operation.
            dependencies_completed = int(responses[1])                    

            # Check if the task is ready for execution. If it is, put it in the "can_now_execute" list. 
            # If the task is not yet ready, then we don't do anything further with it at this point.
            if dependencies_completed == num_dependencies:
                # print("[DEP. CHECKING] - task {} is now ready to execute as all {} dependencies have been computed.".format(invoke_key, num_dependencies))
                can_now_execute.append(invoke_node)
            # else:
                # print("[DEP. CHECKING] - task {} cannot execute yet. Only {} out of {} dependencies have been computed.".format(invoke_key, dependencies_completed, num_dependencies))
                # print("\nMissing dependencies: ")
                # deps = invoke_node.task_payload["dependencies"]
                # for dep_task_key in deps:
                #     if dep_task_key not in self.completed_tasks:
                #         print("     ", dep_task_key)
                # print("\n")
            
        # Invoke all of the ready-to-execute tasks in parallel.
        # print("[ {} ] Invoking {} of the {} downstream tasks of current task {}:".format(datetime.datetime.utcnow(), len(can_now_execute), len(task_node.invoke), task_key))
        # for node in can_now_execute:
        #     print("     ", node.task_key)
        # print("\n")
        for invoke_node in can_now_execute:
            payload = self.serialized_paths[invoke_node.task_key]
            # We can only send a payload of size 256,000 bytes or less to a Lambda function directly.
            # If the payload is too large, then the Lambda will retrieve it from Redis via the key we provide.
            if sys.getsizeof(payload) > 256000:
                payload = json.dumps({"path-key": invoke_node.task_key + "---path"})
            self.lambda_invoker.send(payload)            

    @gen.coroutine
    def deserialize_and_process_message(self, stream, address = None, **kwargs):
        message = None 

        # This uses the dask protocol to break up the message/receive the chunks of the message.
        try:
            n_frames = yield stream.read_bytes(8)
            n_frames = struct.unpack("Q", n_frames)[0]
            lengths = yield stream.read_bytes(8 * n_frames)
            lengths = struct.unpack("Q" * n_frames, lengths)
            
            frames = []
            for length in lengths:
                if length:
                    frame = bytearray(length)
                    n = yield stream.read_into(frame)
                    assert n == length, (n, length)
                else:
                    frame = b""
                frames.append(frame)
        except StreamClosedError as e:
            print("[ {} ] [ERROR] - Stream Closed Error: stream from address {} is closed...".format(datetime.datetime.utcnow(), address))
            print("Real Error: ", e.real_error.__str__())
            raise
        except AssertionError as e:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred on line {} in statement {}. Currently processing a stream from addresss {}.'.format(line, text, address))
            raise 
        else:
            try:
                message = yield from_frames(frames)
            except EOFError:
                print("[ {} ] [ERROR] - Aborted stream on truncated data".format(datetime.datetime.utcnow()))
                raise CommClosedError("aborted stream on truncated data")
        
        # The 'op' (operation) entry specifies what the Proxy should do. 
        op = message["op"]

        yield self.handlers[op](message, address = address, stream = stream)

    @gen.coroutine
    def handle_set(self, message, **kwargs):
        # The "set" operation is sent by Lambda functions so that the proxy can store results in Redis and  
        # possibly invoke downstream tasks, particularly when there is a large fan-out factor for a given node/task.
 
        task_key = message["task-key"]     
        print("[ {} ] [OPERATION - set] Received 'set' operation from a Lambda. Task Key: {}.".format(datetime.datetime.utcnow(), task_key))

        # Grab the associated task node.
        if task_key not in self.path_nodes:
            # This can happen if the Lambda function executes before the proxy finishes processing the DAG info sent by the Scheduler. 
            # In these situations, we add the messages to a list that gets processed once the DAG-processing concludes.
            # print("[ {} ] [WARNING] {} is not currently contained within self.path_nodes... Will try to process again later...".format(datetime.datetime.utcnow(), task_key))
            self.need_to_process.append([task_key, message])
            return
        else:
            # print("[ {} ] The task {} is contained within self.path_nodes. Processing now...".format(datetime.datetime.utcnow(), task_key))
            yield self.process_task(task_key, message)

    @gen.coroutine
    def handle_graph_init(self, message, **kwargs):
        # The "graph_init" operation is sent by the Scheduler to the proxy.
        # This is how the scheduler gives the proxy its address as well as the current payload.
        # print("[ {} ] [OPERATION - graph-init] Graph initialization operation received from Scheduler.".format(datetime.datetime.utcnow()))
        self.scheduler_address = message["scheduler-address"]
            
        # Grab the paths from Redis directly.
        path_keys = message["path-keys"]

        client_keys = defaultdict(list)

        # client_keys = [list() for redis_client in self.redis_clients]

        for key in path_keys:
            client = self.hash_ring.get_node_instance(key[:-7])
            client_keys[client].append(key)

        #for key in path_keys:
        #    hash_obj = hashlib.md5(key[:-7].encode())
        #    val = int(hash_obj.hexdigest(), 16)
        #    idx = val % self.num_redis_clients
        #    client_keys[idx].append(key)

        responses = list()
        for client, keys in client_keys.items():
            res = yield client.mget(*keys)
            responses.extend(res)

        #_responses = [list() for redis_client in self.redis_clients]

        #for i in range(len(client_keys)):
        #    client_keys_list = client_keys[i]
        #    if len(client_keys_list) > 0:
        #        redis_client = self.redis_clients[i]
        #        _responses[i] = yield redis_client.mget(*client_keys_list)

        #responses = []
        #for lst in _responses:
        #    for elem in lst:
        #        responses.append(elem)

        # Iterate over all of the serialized paths and deserialize them/deserialize the nodes.
        starting_nodes = len(self.path_nodes)
        for serialized_path in responses:
            path = json.loads(serialized_path)
            starting_node_key = path["starting-node-key"]

            # Map the task key corresponding to the beginning of the path to the path itself.
            self.serialized_paths[starting_node_key] = serialized_path # May want to check if path is already in the list?

            # Iterate over all of the encoded-and-serialized nodes and deserialize them. 
            for task_key, encoded_node in path["nodes-map"].items():
                decoded_node = base64.b64decode(encoded_node)
                deserialized_node = cloudpickle.loads(decoded_node)
                    
                # The 'frames' stuff is related to the Dask protocol. We use Dask's deserialization algorithm here.
                frames = []
                for encoded in deserialized_node.task_payload:
                    frames.append(base64.b64decode(encoded))
                deserialized_task_payload = yield deserialize_payload(frames)
                deserialized_node.task_payload = deserialized_task_payload
                self.path_nodes[task_key] = deserialized_node
        # print("[ {} ] Added {} new nodes to self.path_nodes dictionary. The dictionary now holds {} nodes.".format(datetime.datetime.utcnow(), len(self.path_nodes) - starting_nodes, len(self.path_nodes)))
        #if len(self.need_to_process) > 0:
        #    print("[ {} ] Processing {} messages contained in self.need_to_process".format(datetime.datetime.utcnow(), len(self.need_to_process)))
        #else:
        #    print("[ {} ] There were 0 messages contained within self.need_to_process.".format(datetime.datetime.utcnow()))
        for lst in self.need_to_process:
            task_key = lst[0]
            value_encoded = lst[1]
            # message = lst[1]
            # print("Processing task {} from self.need_to_process...".format(task_key))
            yield self.process_task(task_key, _value_encoded = value_encoded, print_debug = True) # Could also pass *lst.
        # Clear the list.
        self.need_to_process = [] 

    @gen.coroutine
    def handle_start(self, message, **kwargs):
        stream = kwargs["stream"]
        address = kwargs["address"]
        # This operation needs to happen before anything else. Scheduler should be started *after* the proxy is started in order for this to work.
        print("[ {} ] [OPERATION - start] Received 'start' operation from Scheduler.".format(datetime.datetime.utcnow()))
        self.redis_channel_names = message["redis-channel-names"]

        # We need the number of cores available as this determines how many processes total we can have.
        num_cores = multiprocessing.cpu_count()            
        cores_remaining = num_cores - len(self.redis_channel_names)
            
        # Start a certain number of Redis polling processes to listen for results.
        num_redis_processes_to_create = math.ceil(cores_remaining * 0.5)
        print("Creating {} 'Redis Polling' processes.".format(num_redis_processes_to_create))

        self.redis_channel_names_for_proxy = []
        self.base_channel_name = "redis-proxy-"
        for i in range(num_redis_processes_to_create):
            name = self.base_channel_name + str(i)
            self.redis_channel_names_for_proxy.append(name)

        # Create a list to keep track of the processes as well as the Queue object, which we use for communication between the processes.
        self.redis_polling_processes = []
        self.redis_polling_queue = multiprocessing.Queue()
            
        # For each channel, we create a process and store a reference to it in our list.
        for channel_name in self.redis_channel_names_for_proxy:
            redis_polling_process = Process(target = self.poll_redis_process, args = (self.redis_polling_queue, channel_name, self.redis_endpoints[0]))
            redis_polling_process.daemon = True 
            self.redis_polling_processes.append(redis_polling_process)
                
        # Start the processes.
        for redis_polling_process in self.redis_polling_processes:
            redis_polling_process.start()  
                
        self.lambda_invoker = ProxyLambdaInvoker(interval = "2ms", chunk_size = 1, redis_channel_names = self.redis_channel_names, redis_channel_names_for_proxy = self.redis_channel_names_for_proxy, loop = self.loop)
        self.lambda_invoker.start(self.lambda_client, scheduler_address = self.scheduler_address)

        # The Scheduler stores its address 
        self.scheduler_address = yield self.redis_clients[0].get("scheduler-address")

        payload = {"op": "redis-proxy-channels", "num_channels": len(self.redis_channel_names_for_proxy), "base_name": self.base_channel_name}
        print("Payload for Scheduler: ", payload)
        # self.scheduler_comm = yield connect_to_address(self.scheduler_address)
        # self, stream, local_addr, peer_addr
        local_address = "tcp://" + get_stream_address(stream)
        self.scheduler_comm = TCP(stream, local_address, "tcp://" + address[0], deserialize = True)
        print("Writing message to Scheduler...")
        bytes_written = yield self.scheduler_comm.write(payload)
        print("Wrote {} bytes to Scheduler...".format(bytes_written))
        self.loop.spawn_callback(self.consume_redis_queue)
        print("Now for handle comm")
        #payload2 = {"op": "debug-msg", "message": "[ {} ] Goodbye, world!".format(datetime.datetime.utcnow())}
        #yield self.scheduler_comm.write(payload2)
        #self.loop.spawn_callback(self.hello_world)
        yield self.handle_comm(self.scheduler_comm)
        
    @gen.coroutine
    def hello_world(self):
        print("Hello World starting...")
        counter = 0
        while True:
            _now = datetime.datetime.utcnow()
            payload2 = {"op": "debug-msg", "message": "[ {} ] Hello, world {}!".format(_now, str(counter))}
            yield self.scheduler_comm.write(payload2)
            counter = counter + 1
            yield gen.sleep(2)

    def result_from_lambda(self, task_key, value_encoded):  
        print("[ {} ] [OPERATION - set] Received 'set' operation from a Lambda. Task Key: {}.".format(datetime.datetime.utcnow(), task_key))

        # Grab the associated task node.
        if task_key not in self.path_nodes:
            # This can happen if the Lambda function executes before the proxy finishes processing the DAG info sent by the Scheduler. 
            # In these situations, we add the messages to a list that gets processed once the DAG-processing concludes.
            # print("[ {} ] [WARNING] {} is not currently contained within self.path_nodes... Will try to process again later...".format(datetime.datetime.utcnow(), task_key))
            self.need_to_process.append([task_key, value_encoded])
            return
        else:
            # print("[ {} ] The task {} is contained within self.path_nodes. Processing now...".format(datetime.datetime.utcnow(), task_key))
            yield self.process_task(task_key, _value_encoded = value_encoded)
                
    @gen.coroutine 
    def handle_stream(self, stream, address):
        print("[ {} ] Starting established connection with {}".format(datetime.datetime.utcnow(), address))

        io_error = None
        closed = False        

        try:
            while not closed:
                print("[ {} ] Message received from address {}. Handling now...".format(datetime.datetime.utcnow(), address))
                yield self.deserialize_and_process_message(stream, address = address)
        except (CommClosedError, EnvironmentError) as e:
            io_error = e
            closed = True 
        except StreamClosedError as e:
            print("[ERROR] Stream closed")
            print("Real Error: ", e.real_error.__str__())
            closed = True 
        except AssertionError as e:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred in file {} on line {} in statement "{}". Currently processing a stream from addresss {}.'.format(filename, line, text, address))
            raise
        except Exception as e:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred in file {} on line {} in statement "{}". Currently processing a stream from addresss {}.'.format(filename, line, text, address))
            raise 
        finally:
            stream.close()
            assert stream.closed()
    
    @gen.coroutine
    def handle_comm(self, comm, extra=None, every_cycle=[]):
        extra = extra or {}
        print("[ {} ] Starting established TCP Comm connection with {}".format(datetime.datetime.utcnow(), comm._peer_addr))

        io_error = None
        closed = False
        while True:
            try:
                msg = yield comm.read()

                if not isinstance(msg, dict):
                    raise TypeError(
                        "Bad message type.  Expected dict, got\n  " + str(msg) + " of type " + str(type(msg))
                    )
                try:
                    op = msg.pop("op")
                except KeyError:
                    raise ValueError(
                        "Received unexpected message without 'op' key: " % str(msg)
                    )
                yield self.handlers[op](msg)
                close_desired = msg.pop("close", False)
                msg = result = None
                if close_desired:
                    print("[ {} ] Close desired. Closing comm.".format(datetime.datetime.utcnow()))
                    yield comm.close()
                if comm.closed():
                    break
            except (CommClosedError, EnvironmentError) as e:
                io_error = e
                print("[ {} ] [ERROR] CommClosedError, EnvironmentError: {}".format(datetime.datetime.utcnow(), e.__str__()))
                raise 
                break
            except Exception as e:
                print("[ {} ] [ERROR] Exception: {}".format(datetime.datetime.utcnow(), e.__str__()))
                raise 
                break

    def get_redis_client(self, task_key):
        hash_obj = hashlib.md5(task_key.encode())
        val = int(hash_obj.hexdigest(), 16)
        idx = val % self.num_redis_clients
        return self.redis_clients[idx]

    def poll_redis_process(self, _queue, redis_channel_name, redis_endpoint):
        ''' This function defines a process which continually polls Redis for messages. 
        
            When a message is found, it is passed to the main Scheduler process via the queue given to this process. ''' 
        print("Redis Polling Process started. Polling channel ", redis_channel_name)
        
        IP, port = redis_endpoint 

        redis_client = redis.StrictRedis(host=IP, port = port, db = 0)

        print("[ {} ] Redis polling processes connected to Redis Client at {}:{}".format(datetime.datetime.utcnow(), IP, port))

        base_sleep_interval = 0.05 
        max_sleep_interval = 0.15 
        current_sleep_interval = base_sleep_interval
        consecutive_misses = 0
        
        # We just do pub-sub on the first redis client.
        redis_channel = redis_client.pubsub(ignore_subscribe_messages = True)
        redis_channel.subscribe(redis_channel_name)
        
        # This process will just loop endlessly polling Redis for messages. When it finds a message,
        # it will decode it (from bytes to UTF-8) and send it to the Scheduler process via the queue. 
        #
        # If no messages are found, then the thread will sleep before continuing to poll. 
        while True:
            message = redis_channel.get_message()
            if message is not None:
                timestamp_now = datetime.datetime.utcnow()
                # print("[ {} ] Received message from channel {}.".format(timestamp_now, redis_channel_name))   
                data = message["data"]
                # The message should be a "bytes"-like object so we decode it.
                # If we neglect to turn off the subscribe/unsubscribe confirmation messages,
                # then we may get messages that are just numbers. 
                # We ignore these by catching the exception and simply passing.
                data = data.decode()
                data = json.loads(data)
                # print("Data: ", data)
                _queue.put([data])
                consecutive_misses = 0
                current_sleep_interval = base_sleep_interval
            else:
                time.sleep(current_sleep_interval)
                consecutive_misses = consecutive_misses + 1
                current_sleep_interval += 0.05 
                if (current_sleep_interval > max_sleep_interval):
                    current_sleep_interval = max_sleep_interval

    @gen.coroutine
    def consume_redis_queue(self):
        ''' This function executes periodically (as a PeriodicCallback on the IO loop). 
        It reads messages from the message queue until none are available.'''
        # print("Consume Redis Queue is being executed...")
        while True:
            messages = []
            # 'end' is the time at which we should stop iterating. By default, it is 50ms.
            stop_at = datetime.datetime.utcnow().microsecond + 5000
            while datetime.datetime.utcnow().microsecond < stop_at and len(messages) < 50:
                try:
                    timestamp_now = datetime.datetime.utcnow()
                    # Attempt to get a payload from the Queue. A 'payload' consists of a message
                    # and possibly some benchmarking data. The message will be at index 0 of the payload.
                    payload = self.redis_polling_queue.get(block = False, timeout = None)
                    message = payload[0]
                    messages.append(message)
                # In the case that the queue is empty, break out of the loop and process what we already have.
                except queue.Empty:
                    break
            if len(messages) > 0:
                # print("[ {} ] Processing {} messages from Redis Message Queue.".format(datetime.datetime.utcnow(), len(messages)))
                for msg in messages:
                    if "op" in msg:
                        op = msg.pop("op")
                        if op == "set":
                            task_key = msg.pop("task-key")
                            value_encoded = msg.pop("value-encoded")
                            print("[ {} ] [OPERATION - set] Received 'set' operation from a Lambda. Task Key: {}.".format(datetime.datetime.utcnow(), task_key))

                            # Grab the associated task node.
                            if task_key not in self.path_nodes:
                                # This can happen if the Lambda function executes before the proxy finishes processing the DAG info sent by the Scheduler. 
                                # In these situations, we add the messages to a list that gets processed once the DAG-processing concludes.
                                # print("[ {} ] [WARNING] {} is not currently contained within self.path_nodes... Will try to process again later...".format(datetime.datetime.utcnow(), task_key))
                                self.need_to_process.append([task_key, value_encoded])
                                continue 
                            else:
                                # print("[ {} ] The task {} is contained within self.path_nodes. Processing now...".format(datetime.datetime.utcnow(), task_key))
                                yield self.process_task(task_key, _value_encoded = value_encoded)
                        else:
                            print("[ {} ] [ERROR] Unknown Operation from Redis Queue... Message: {}".format(datetime.datetime.utcnow(), msg))
                    else:
                        print("[ {} ] [ERROR] Message from Redis Queue did NOT contain an operation... Message: {}".format(datetime.datetime.utcnow(), msg))
            # Sleep for 5 milliseconds...
            yield gen.sleep(0.005)
@gen.coroutine
def deserialize_payload(payload):
   msg = yield from_frames(payload)
   raise gen.Return(msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some values.')
    parser.add_argument("-res", "--redis-endpoints", dest="redis_endpoints", nargs = "*", help="Redis endpoint IP addresses", default = ["127.0.0.1"])
    parser.add_argument("-rps", "--redis-ports", dest="redis_ports", nargs = "*", type=int, default = [6379, 6380])
    args = vars(parser.parse_args())
    redis_endpoints = args["redis_endpoints"]
    redis_ports = args["redis_ports"]
    # If all the servers are listening on the same port, we can just specify the port once.
    if len(redis_ports) == 1 and len(redis_endpoints) > 1:
        redis_ports = redis_ports * len(redis_endpoints)
    redis_endpoints = list(zip(redis_endpoints, redis_ports))
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    proxy = RedisProxy(redis_endpoints, lambda_client)
    proxy.start()    