from collections import defaultdict

# All of the possible event names.
event_names = ["Invoke Lambda", "Execute Task", "Redis Read", "Redis Write", "P2P Send", "P2P Receive", "P2P Connect", "P2P Poll", "SQS Send Message", "SQS Receive Message", "SQS Create Queue", "SQS Delete Queue", "SQS Get Queue URL", "Serialize", "Deserialize", "Coordinator Connect", "Coordinator Send", "Coordinator Receive"]

class WukongEvent(object):
    """
    Used to record events that occur during execution. Intended to be plotted in a Gantt chart.
    
    Attributes:
        name (str)     : Name of the event (e.g., "SQS Send Message")
        start (int)    : Start time of the event.
        end (int)      : End time of the event.
        duration (int) : Duration of the event (in seconds).
    """
    def __init__(self, name = None, start_time = None, end_time = None):
        self.name = name
        self.start_time = start_time
        self.end_time = end_time
        self.duration = end_time - start_time 

class TaskExecutionBreakdown(object):
    # Basically just a container for the breakdown of a task's execution on a Lambda function. 
    # There are some imperfections in the record-taking.
    def __init__(
         self,
         task_key,                              # Key of the corresponding task
         redis_read_time = 0, 
         dependency_processing = 0,             # Deserializing dependencies and all that
         task_execution_start_time = 0,         # Time stamp that execution of task code started
         task_execution = 0,                    # Length of executing task code 
         task_execution_end_time = 0,           # Time stamp that execution of task code ended
         redis_write_time = 0,          
         invoking_downstream_tasks = 0,         # invoking ready-to-execute dependent tasks 
         total_time_spent_on_this_task = 0,     # how long we spent processing this task
         task_processing_start_time = 0,        # timestamp of when we started processing the task
         task_processing_end_time = 0,          # timestamp of when we finished processing the task
         update_graph_id = None):         
        self.task_key = task_key
        self.redis_read_time = redis_read_time
        self.redis_write_time = redis_write_time
        self.task_execution = task_execution
        self.checking_and_incrementing_dependency_counters = 0
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.dependency_processing = dependency_processing
        self.serialization_time = 0
        self.publishing_messages = 0
        self.deserialization_time = 0       
        self.task_execution = task_execution

        # P2P Metrics
        self.p2p_pairing_time = 0               # Time spent calling pair()
        self.p2p_send_time = 0                  # Time spent calling socket.send()
        self.p2p_receive_time = 0               # Time spent calling socket.recv()
        self.p2p_num_pairings = 0               # Number of times pair() was called.
        self.p2p_num_messages_sent = 0          # Number of P2P messages sent.
        self.p2p_num_messages_received = 0      # Number of P2P messages received.        

        # SQS Metrics
        self.sqs_send_message_time = 0          # Time spent calling sqs.send_message().
        self.sqs_receive_message_time = 0       # Time spent calling sqs.receive_message().
        self.sqs_create_queue_time = 0          # Time spent calling sqs.create_queue().
        self.sqs_delete_queue_time = 0          # Time spent calling sqs.delete_queue().
        self.sqs_get_queue_url_time = 0         # Time spent calling sqs.get_queue_url().

        self.task_execution_start_time = task_execution_start_time
        self.task_execution_end_time = task_execution_end_time
        self.total_time_spent_on_this_task = total_time_spent_on_this_task
        self.task_processing_start_time = task_processing_start_time
        self.task_processing_end_time = task_processing_end_time 
        self.update_graph_id = update_graph_id

class LambdaExecutionBreakdown(object):
    """
        .. attribute:: start_time 
        
            The time at which the Lambda function began execution.

        .. attribute:: process_path_time

            The time spent executing the 'process_path' function.

        .. attribute:: process_task_time

            The total time the Lambda function spent processing tasks themselves. This essentially
            is just a sum of all execution times of the 'process_task' function.

        .. attribute:: checking_and_incrementing_dependency_counters

            The time spent checking if downstream tasks are ready to execute. This includes incrementing/checking dependency counters.

        .. attribute:: redis_read_time

            Time spent writing data to Redis. This does NOT include checking dependency counters.

        .. attribute:: redis_write_time

            Time spent reading data from Redis. This does NOT include incrementing dependency counters.

        .. attribute:: publishing_messages

            Time spent sending a message to the proxy via 'publish'.

        .. attribute:: invoking_downstream_tasks   

            Time spent calling the boto3 API's 'invoke' function.

        .. attribute:: number_of_tasks_executed

            The number of tasks executed directly by this Lambda function.

        .. attribute:: serialization_time

            The time spent serializing data.

        .. attribute:: deserialization_time

            The time spent deserializing data.

        .. attribute:: total_duration               

            Total runtime of the Lambda function.

        .. attribute:: execution_time

            Time spent explicitly executing tasks.

        .. attribute:: redis_read_times

            Dictionary mapping task keys to the amount of time spent reading them.

        .. attribute:: redis_write_times                              

            Dictionary mapping task keys to the amount of time spent writing them.

        .. attribute:: tasks_pulled_down

            How many tasks this Lambda "pulled down" to execute locally (after executing a "big" task).

        .. attribute:: write_counters

            Mapping of task keys to how many time data was written to that key.

        .. attribute:: read_counters

            Mapping of task keys to how many time data was read from that key.

        .. attribute:: aws_request_id     

            The AWS Request ID of this Lambda function.

        .. attribute:: fan_outs

            Fan-out data for each fan-out processed by this Lambda.

        .. attribute:: fan_ins                          

            Fan-in data for each fan-in processed by this Lambda.
        
        .. attribute:: install_deps_from_S3

            Time it takes to download and unzip dependencies from S3.
    """
    def __init__(
         self,
         start_time = 0,
         process_path_time = 0,
         process_task_time = 0,
         redis_read_time = 0,
         redis_write_time = 0,
         invoking_downstream_tasks = 0,
         number_of_tasks_executed = 0,
         total_duration = 0,
         aws_request_id = None):
        self.start_time = start_time
        self.number_of_tasks_executed = number_of_tasks_executed
        self.process_path_time = process_path_time
        self.process_task_time = process_task_time
        self.checking_and_incrementing_dependency_counters = 0
        self.redis_read_time = redis_read_time
        self.redis_write_time = redis_write_time
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.publishing_messages = 0
        self.serialization_time = 0
        self.deserialization_time = 0
        self.execution_time = 0
        self.total_duration = total_duration

        # P2P Metrics
        self.p2p_pairing_time = 0               # Time spent calling pair()
        self.p2p_send_time = 0                  # Time spent calling socket.send()
        self.p2p_receive_time = 0               # Time spent calling socket.recv()
        self.p2p_num_pairings = 0               # Number of times pair() was called.
        self.p2p_num_messages_sent = 0          # Number of P2P messages sent.
        self.p2p_num_messages_received = 0      # Number of P2P messages received. 

        # SQS Metrics
        self.sqs_send_message_time = 0          # Time spent calling sqs.send_message().
        self.sqs_receive_message_time = 0       # Time spent calling sqs.receive_message().
        self.sqs_create_queue_time = 0          # Time spent calling sqs.create_queue().
        self.sqs_delete_queue_time = 0          # Time spent calling sqs.delete_queue().
        self.sqs_get_queue_url_time = 0         # Time spent calling sqs.get_queue_url().

        self.p2p_poll_time = 0                  # Time spent calling UDTEpoll.wait()

        # Websockets Metrics
        self.websockets_connect_time = 0        # Time spent connecting to NAT Punch coordinator service.
        self.websockets_send_time = 0           # Time spent sending data to the NAT Punch coordinator service.
        self.websockets_recv_time = 0           # Time spent receiving data from the NAT Punch coordinator.

        self.events = list()

        # Collections of Metrics (instead of single variables, these are often dicts/arrays)  
        # Map from Task Key --> (Size of Data, Time) 
        self.redis_read_times = dict()
        # Map from Task Key --> (Size of Data, Time) 
        self.redis_write_times = dict()
        self.install_deps_from_S3 = 0
        self.tasks_pulled_down = 0
        self.reuse_count = 0
        self.write_counters = defaultdict(int)
        self.read_counters = defaultdict(int)
        self.aws_request_id = aws_request_id 
        self.fan_outs = list() # List where we keep track of task sizes in the context of fan-outs.
        self.fan_ins = list()  # List where we keep track of task sizes in the context of fan-ins.

    def add_event(self, event):
        self.events.append(event)

    def add_write_time(self, fargateARN, data_key, size, _redis_write_duration, start_time, stop_time):
        """ Add an entry to the redis_write_times dictionary.
        
                data_key (str)              -- The task/path/etc. we're entering data for.
                size (int)                  -- The size of the data being written.
                _redis_write_duration (int) -- The time the write operation took to complete.
        """
        # I do not think the same task key could be added more than once for writes, but just in case,
        # I have code to handle the case where a value already exists (we just use a list of values
        # for such a situation).
        if data_key in self.redis_write_times:
            count = self.write_counters[data_key]
            count += 1
            self.write_counters[data_key] = count
            data_key = data_key + "---" + str(count)
            self.redis_write_times[data_key] = {
                "size": size,
                "duration": _redis_write_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN
            }
            # (size, _redis_write_duration, start_time, stop_time)
        else:
            self.redis_write_times[data_key] = {
                "size": size,
                "duration": _redis_write_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN                
            }
            # (size, _redis_write_duration, start_time, stop_time)

    def add_read_time(self, fargateARN, data_key, size, _redis_read_duration, start_time, stop_time):
        """ Add an entry to the redis_write_times dictionary.
        
                data_key (str)              -- The task/path/etc. we're entering data for.
                size (int)                  -- The size of the data being read.
                _redis_write_duration (int) -- The time the read operation took to complete.
        """
        # For read times, there can absolutely be multiple values per key.
        if data_key in self.redis_read_times:
            count = self.read_counters[data_key]
            count += 1
            self.read_counters[data_key] = count
            data_key = data_key + "---" + str(count)
            self.redis_write_times[data_key] = {
                "size": size,
                "duration": _redis_read_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN              
            }
            # (size, _redis_read_duration, start_time, stop_time)
        else:
            self.redis_read_times[data_key] = {
                "size": size,
                "duration": _redis_read_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN              
            }
            # (size, _redis_read_duration, start_time, stop_time)