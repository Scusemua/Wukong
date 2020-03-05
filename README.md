# Wukong

A fast and efficient serverless DAG engine.

Paper: In Search of a Fast and Efficient Serverless DAG Engine
https://arxiv.org/abs/1910.05896

This branch contains what is roughly the latest version of Wukong. Of course, this is still a work-in-progress. There are likely to be bugs.

<!--- ## What's new in this branch?

<!--- This version of Wukong adopts a "tiered" approach to Redis -- it uses a bunch of VMs, generally multiple hosted on the same machine. Some VMs are reserved for "large" objects while others are used exclusively for "small" objects. The "large" object threshold is a configurable paramater on the Static Scheduler. 

<!--- There is a script `CreateAndPrintRedisInfo.py` which greatly simplifies the process of provisioning Redis. You give this script the ports you wish to use, and the script will create the Redis instances for you. It gets the IPs using the EC2 AWS API. It automatically selects the VM to be used as the Scheduler and the proxy, also. Currently you cannot specify this yourself. The script does not start either of these applications on the VMs, but it reserves two IP addresses to use for the Scheduler and the proxy.

<!---The script also generates boiler plate code that you can use when starting Wukong (both the Static Scheduler and the KV Store Proxy). You can also copy-and-paste a portion of the code generated for the Static Scheduler to use with the Lambda functions. You may need to modify the commands that are executed in the script to target your OS. I've been using it with the [Amazon Linux AMI](https://aws.amazon.com/amazon-linux-ami/). (The Amazon Linux AMI is what all of this has been tested on.)

<!---If you need any help using the script, please contact me. It isn't exactly cleaned up for public use -- it's just something we've been using to make our lives easier. There is some explanation at the top of the file, but I suspect it will still be hard to use the script for anyone else. 

<!--- The code for the AWS Lambda function (i.e., Task Executor) has also changed considerably. There are a number of changes aside from the changes to Redis. For one, when a Task Executor completes a task whose output data is large, it will attempt to execute downstream tasks locally until the output data is small. This is done to minimize the amount of network communication performed for large objects. 

<!---At a high level, there are two distinct ways tasks are processed after being executed. The way that the task is processed depends on whether or not its intermediate output data is large or small (based on a user-defined threshold). If it is small, then the processing is basically the same as before (and occurs in the `process_small` function). If the data is large, then, as mentioned, the Lambda will attempt to execute downstream tasks locally if at all possible. This is done in the `process_big` function. 

<!--- The `chunk_task_threshold` parameter to `LocalCluster` is currently being used to determine whether intermediate data is large enough to warrant executing downstream tasks locally or not. If `size(DATA) >= chunk_task_threshold` then the downstream tasks would be executed locally if circumstances permit (i.e., they're ready to execute). Note that the units of `chunk_task_threshold` are bytes. 

<!--- As far as running Wukong goes, the Lambda function (Task Executor) now needs to be given a hard-coded list of the "large object" Redis shards and the "small object" Redis shards. The format for these lists is `[(IP_1, Port_1), (IP_2, Port_2), ..., (IP_n, Port_n)]`. The variables are at the top of `function.py` in the "AWS Lambda Task Executor" folder. 

<!--- The command for starting the KV Store Proxy has also changed. If you use the script mentioned above, then the command will be generated for you. (You'll just need to copy-and-paste it into your terminal). 

<!--- If you encounter any issues understanding or running this version of Wukong, please contact us and we'll be happy to help troubleshoot or explain. We will also be updating the documentation as time goes on.

## What is Wukong?

Wukong is a serverless DAG scheduler attuned to AWS Lambda. Wukong provides decentralized scheduling using a combination of static and dynamic scheduling. Wukong supports general Python data analytics workloads. 

<!--- ![Architecture](https://i.imgur.com/XXXXXX.png "Wukong's Architecture") --->

## Code Overview/Explanation 

*This section is currently under development...*

While this branch contains the latest code release, the documentation in this readme is not necessarily up-to-date. The code itself has a fair amount of documentation (in particular, the AWS Lambda Task Executor has a large amount of comments) though. 

### The Static Scheduler

Generally speaking, a user submits a job by calling the `.compute()` function on an underlying Dask collection. Support for Dask's asynchronous `client.compute()` API is coming soon.

When the `.compute()` function is called, the `update_graph()` function is called within the static Scheduler, specifically in **scheduler.py**. This function is responsible for adding computations to the Scheduler's internal graph. It's triggered whenever a Client calls `.submit()`, `.map()`, `.get()`, or `.compute()`. The depth-first search (DFS) method is defined with the `update_graph` function, and the DFS also occurs during the `update_graph` function's execution. 

Once the DFS has completed, the Scheduler will serialize all of the generated paths and store them in the KV Store (Redis). Next, the Scheduler will begin the computation by submitting the leaf tasks to the `BatchedLambdaInvoker` object (which is defined in **batched_lambda_invoker.py**. The "Leaf Task Invoker" processes are defined within the `BatchedLambdaInvoker` class as the `invoker_polling_process` function. Additionally, the `_background_send` function is running asynchronously on an interval (using [Tornado](https://www.tornadoweb.org/en/stable/)). This function takes whatever tasks have been submitted by the Scheduler and divdes them up among itself and the Leaf Task Invoker processes, which then invoke the leaf tasks. 

The Scheduler listens for results from Lambda using a "Subscriber Process", which is defined by the `poll_redis_process` function. This process is created in the Scheduler's `start` function. (All of this is defined in **scheduler.py**.) The Scheduler is also executing the `consume_redis_queue()` function asynchronously (i.e., on the [Tornado IOLoop](https://www.tornadoweb.org/en/stable/ioloop.html)). This function processes whatever messages were received by the aforementioned "Subscriber Process(es)". Whenever a message is processed, it is passed to the `result_from_lambda()` function, which begins the process of recording the fact that a "final result" is available. 

### The KV Store Proxy

This component is used to parallelize Lambda function invocations in the middle of a workload's execution.
...

### The AWS Lambda Task Executor

The Task Executors are responsible for executing tasks and performing dynamic scheduling. 
...

### Developer Setup Notes

When setting up Wukong, make sure to update the variables referencing the name of the AWS Lambda function used as the Wukong Task Executor. For example, in "AWS Lambda Task Executor/function.py", this is a variable *lambda_function_name* whose value should be the same as the name of the Lambda function as defined in AWS Lambda itself.

There is also a variable referencing the function's name in "Static Scheduler/distributed/batched_lambda_invoker.py" (as a keyword argument to the constructor of the BatchedLambdaInvoker object) and in "KV Store Proxy/proxy_lambda_invoker.py" (also as a keyword argument to the constructor of ProxyLambdaInvoker).

By default, Wukong is configured to run within the us-east-1 region. If you would like to use a different region, then you need to pass the "region_name" parameter to the Lambda Client objects created in "Static Scheduler/distributed/batched_lambda_invoker.py", "KV Store Proxy/proxy_lambda_invoker.py", "KV Store Proxy/proxy.py", "AWS Lambda Task Executor/function.py", and "Static Scheduler/distributed/scheduler.py".

<!--- There is a script `CreateAndPrintRedisInfo.py` which greatly simplifies the process of provisioning Redis. A description of the script is provided above in the "What's new in this branch?" section. ---> 

The AWS Lambda function requires a few layers to run. These layers can be included directly with the following ARN's:

arn:aws:lambda:us-east-1:561589293384:layer:DaskLayer2:2
arn:aws:lambda:us-east-1:561589293384:layer:DaskDependenciesAndXRay:6
arn:aws:lambda:us-east-1:561589293384:layer:dask-ml-layer:9

## Code Examples

In the following examples, modifying the value of the *chunks* parameter will essentially change the granularity of the tasks generated in the DAG. Essentially, *chunks* specifies how the initial input data is partitioned. Increasing the size of *chunks* will yield fewer individual tasks, and each task will operate over a large proportion of the input data. Decreasing the size of *chunks* will result in a greater number of individual tasks, with each task operating on a smaller portion of the input data. 

### LocalCluster Overview
```
LocalCluster(object):
  host : string
    The public DNS IPv4 address associated with the EC2 instance on which the Scheduler process is executing, along with the port on 
    which the Scheduler is listening. The format of this string should be "IPv4:port". 
  n_workers : int,
    Artifact from Dask. Leave this at zero.
  proxy_adderss : string,
    The public DNS IPv4 address associated with the EC2 instance on which the KV Store Proxy process is executing.
  proxy_port : 8989,
    The port on which the KV Store Proxy process is listening.
  num_lambda_invokers : int
    This value specifies how many 'Initial Task Executor Invokers' should be created by the Scheduler. The 'Initial Task 
    Executor Invokers' are processes that are used by the Scheduler to parallelize the invocation of Task Executors
    associated with leaf tasks. These are particularly useful for large workloads with a big number of leaf tasks.
  max_task_fanout : int
    This specifies the size of a "fanout" required for a Task Executor to utilize the KV Store Proxy for parallelizing downstream
    task invocation. The principle here is the same as with the initial task invokers. Our tests found that invoking Lambda functions
    takes about 50ms on average. As a result, if a given Task T has a large fanout (i.e., there are a large number of downstream tasks 
    directly dependent on T), then it may be advantageous to parallelize the invocation of these downstream tasks.
  big_task_threshold : int
    The threshold (in bytes) for determining whether or not to execute downstream tasks locally. More specifically, if the intermediate
    output data of a task is greater than or equal to 'chunk_task_threshold', then the Task Executor will attempt to execute the task's
    downstream tasks locally. This limits the amount of large-object network communication that has to occur, thereby speeding up the
    execution of the workload considerably.
```

### SVD of 'Tall-and-Skinny' Matrix 
```python
import dask.array as da
from distributed import LocalCluster, Client
lc = LocalCluster(host='ec2-XX-XX-XXX-X.compute-1.amazonaws.com:8786',
                  proxy_address = 'ec2-XX-XXX-X-XXX.compute-1.amazonaws.com',
                  n_workers = 0,
                  proxy_port = 8989,
                  num_lambda_invokers = 30,
                  chunk_large_tasks = False,
                  big_task_threshold = 200_000_000,
                  max_task_fanout = 330,
                  num_fargate_nodes = 25, 
                  use_bit_dep_checking = True,
                  executors_use_task_queue = True,
                  ecs_task_definition = 'MyECSTaskDefinition',
                  aws_region = "us-east-1") 
client = Client(local_cluster)

# Compute the SVD of 'Tall-and-Skinny' Matrix 
X = da.random.random((200000, 1000), chunks=(10000, 1000))
u, s, v = da.linalg.svd(X)

# Start the computation.
v.compute()
```

### SVD of Square Matrix with Approximation Algorithm
```python
import dask.array as da
from distributed import LocalCluster, Client
lc = LocalCluster(host='ec2-XX-XX-XXX-X.compute-1.amazonaws.com:8786',
                  proxy_address = 'ec2-XX-XXX-X-XXX.compute-1.amazonaws.com',
                  n_workers = 0,
                  proxy_port = 8989,
                  num_lambda_invokers = 30,
                  chunk_large_tasks = False,
                  big_task_threshold = 200_000_000,
                  max_task_fanout = 330,
                  num_fargate_nodes = 25, 
                  use_bit_dep_checking = True,
                  executors_use_task_queue = True,
                  ecs_task_definition = 'MyECSTaskDefinition',
                  aws_region = "us-east-1")  
client = Client(local_cluster)

# Compute the SVD of 'Tall-and-Skinny' Matrix 
X = da.random.random((10000, 10000), chunks=(2000, 2000))
u, s, v = da.linalg.svd_compressed(X, k=5)

# Start the computation.
v.compute()
```

### Tree Reduction
``` python
from dask import delayed 
import operator 
from distributed import LocalCluster, Client
lc = LocalCluster(host='ec2-XX-XX-XXX-X.compute-1.amazonaws.com:8786',
                  proxy_address = 'ec2-XX-XXX-X-XXX.compute-1.amazonaws.com',
                  n_workers = 0,
                  proxy_port = 8989,
                  num_lambda_invokers = 30,
                  chunk_large_tasks = False,
                  big_task_threshold = 200_000_000,
                  max_task_fanout = 330,
                  num_fargate_nodes = 25, 
                  use_bit_dep_checking = True,
                  executors_use_task_queue = True,
                  ecs_task_definition = 'MyECSTaskDefinition',
                  aws_region = "us-east-1") 
client = Client(local_cluster)

L = range(1024)
while len(L) > 1:
  L = list(map(delayed(operator.add), L[0::2], L[1::2]))

# Start the computation.
L[0].compute()
```

### GEMM (Matrix Multiplication) 
``` python
import dask.array as da
from distributed import LocalCluster, Client
lc = LocalCluster(host='ec2-XX-XX-XXX-X.compute-1.amazonaws.com:8786',
                  proxy_address = 'ec2-XX-XXX-X-XXX.compute-1.amazonaws.com',
                  n_workers = 0,
                  proxy_port = 8989,
                  num_lambda_invokers = 30,
                  chunk_large_tasks = False,
                  big_task_threshold = 200_000_000,
                  max_task_fanout = 330,
                  num_fargate_nodes = 25, 
                  use_bit_dep_checking = True,
                  executors_use_task_queue = True,
                  ecs_task_definition = 'MyECSTaskDefinition',
                  aws_region = "us-east-1") 
client = Client(local_cluster)

x = da.random.random((10000, 10000), chunks = (1000, 1000))
y = da.random.random((10000, 10000), chunks = (1000, 1000))
z = da.matmul(x, y)

# Start the computation.
z.compute() 
```

### Parallelizing Prediction (sklearn.svm.SVC)
``` python
import pandas as pd
import seaborn as sns
import sklearn.datasets
from sklearn.svm import SVC

import dask_ml.datasets
from dask_ml.wrappers import ParallelPostFit
from distributed import LocalCluster, Client
lc = LocalCluster(host='ec2-XX-XX-XXX-X.compute-1.amazonaws.com:8786',
                  proxy_address = 'ec2-XX-XXX-X-XXX.compute-1.amazonaws.com',
                  n_workers = 0,
                  proxy_port = 8989,
                  num_lambda_invokers = 30,
                  chunk_large_tasks = False,
                  big_task_threshold = 200_000_000,
                  max_task_fanout = 330,
                  num_fargate_nodes = 25, 
                  use_bit_dep_checking = True,
                  executors_use_task_queue = True,
                  ecs_task_definition = 'MyECSTaskDefinition',
                  aws_region = "us-east-1")  
client = Client(local_cluster)

X, y = sklearn.datasets.make_classification(n_samples=1000)
clf = ParallelPostFit(SVC(gamma='scale'))
clf.fit(X, y)

X, y = dask_ml.datasets.make_classification(n_samples=800000,
                                            random_state=800000,
                                            chunks=800000 // 20)

# Start the computation.
clf.predict(X).compute()

```
