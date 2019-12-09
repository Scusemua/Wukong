from collections import defaultdict

class TaskExecutionBreakdown(object):
    # Basically just a container for the breakdown of a task's execution on a Lambda function. 
    # There are some imperfections in the record-taking. For 'dependency_checking', this is only 
    # collected for "Begin" tasks. 
    def __init__(
         self,
         task_key,                              # Key of the corresponding task
         dependency_checking = 0,               # Non-zero for 'begin' tasks only; time spent checking if deps are ready
         redis_read_time = 0, 
         dependency_processing = 0,             # Deserializing dependencies and all that
         task_execution_start_time = 0,         # Time stamp that execution of task code started
         task_execution = 0,                    # Length of executing task code 
         task_execution_end_time = 0,           # Time stamp that execution of task code ended
         redis_write_time = 0,          
         process_downstream_tasks_time = 0,     # checking deps of dependent tasks 
         invoking_downstream_tasks = 0,         # invoking ready-to-execute dependent tasks 
         total_time_spent_on_this_task = 0,     # how long we spent processing this task
         task_processing_start_time = 0,        # timestamp of when we started processing the task
         task_processing_end_time = 0):         # timestamp of when we finished processing the task
        self.task_key = task_key
        self.dependency_checking = dependency_checking
        self.redis_read_time = redis_read_time
        self.dependency_processing = dependency_processing
        self.task_execution_start_time = task_execution_start_time
        self.task_execution_end_time = task_execution_end_time
        self.task_execution = task_execution
        self.redis_write_time = redis_write_time
        self.process_downstream_tasks_time = process_downstream_tasks_time
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.total_time_spent_on_this_task = total_time_spent_on_this_task
        self.task_processing_start_time = task_processing_start_time
        self.task_processing_end_time = task_processing_end_time

class LambdaExecutionBreakdown(object):
    def __init__(
         self,
         start_time = 0,
         process_path_time = 0,
         process_task_time = 0,
         process_downstream_tasks_time = 0,
         redis_read_time = 0,
         redis_write_time = 0,
         invoking_downstream_tasks = 0,
         number_of_tasks_executed = 0,
         total_duration = 0,
         aws_request_id = None):
        self.start_time = start_time
        self.process_path_time = process_path_time
        self.process_task_time = process_task_time
        self.process_downstream_tasks_time = process_downstream_tasks_time
        self.redis_read_time = redis_read_time
        self.redis_write_time = redis_write_time
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.number_of_tasks_executed = number_of_tasks_executed
        self.total_duration = total_duration
        # Map from Task Key --> (Size of Data, Time) 
        self.redis_read_times = dict()
        # Map from Task Key --> (Size of Data, Time) 
        self.redis_write_times = dict()
        self.tasks_pulled_down = 0
        self.write_counters = defaultdict(int)
        self.read_counters = defaultdict(int)
        self.aws_request_id = aws_request_id 

    def add_write_time(self, data_key, size, _redis_write_duration, start_time, stop_time):
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
            self.redis_write_times[data_key] = (size, _redis_write_duration, start_time, stop_time)
        else:
            self.redis_write_times[data_key] = (size, _redis_write_duration, start_time, stop_time)

    def add_read_time(self, data_key, size, _redis_read_duration, start_time, stop_time):
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
            self.redis_write_times[data_key] = (size, _redis_read_duration, start_time, stop_time)
        else:
            self.redis_read_times[data_key] = (size, _redis_read_duration, start_time, stop_time)