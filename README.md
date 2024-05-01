# lariat-agents

Lariat Data (www.lariatdata.com) is a Continuous Data Quality Monitoring Platform 
to ensure data products don't break even as business logic, 
input data and infrastructure change. 

With Lariat you can define, extract and visualize metrics about data quality. 

Lariat Agents are responsible for: 
1. Getting defined indicators (data quality metrics) from the Lariat Platform
2. Extracting indicator values by running the relevant code and queries on the desired data source (e.g. Snowflake, AWS Athena)
3. Writing the indicator values and timestamps to a sink (generally back to Lariat) so that the data can be consumed for visualization and alerting

Note: The Agent only sinks diagnostic data (timestamp, value) pair, and does not egress 
raw data or PII data. 

We have currently open-sourced the following agents: 

- Snowflake
- Athena 
- S3 Object Monitoring  

Below are some agents we are in the process of open sourcing:
- Postgres
- Kafka 
- Parquet files 
- Pandas 
- Flask 
- Big Query 

Please direct any questions about supported agents to info@lariatdata.com 
or via a GitHub issue. 

## Repository Structure 

### lariat_agents 

- `constants.py`: Contains the environment variables and constant vars used across agents 
- `base` : This path contains the base classes that represent the core functions performed by the agent. More detail [here](#agent-structure)
- `agent`: This contains implementations of the agent type. The first sub-folder (e.g. `snowflake/`) represents the agent type. This also contains the Dockerfile, executor (e.g. Lambda app, Azure Function) and requirements.txt to run the concrete agent. 

### lariat_python_common 
This section of the repository includes convenience code that is factored out in order to keep the agent more readable. 


## Agent Structure 

The core components of the agent are: 

#### (batch or streaming) base_agent: 
The BatchBaseAgent and StreamingBaseAgent class are responsible for being the control center of the Lariat Agent. 
It is where the actions for retrieving and running indicators, along with getting schemata are done. 

Most of this abstract class has implemented functions that will not need to be reimplemented (i.e. gathering definitions from Lariat, pushing schemas out to Lariat, optimized query grouping )

Note that an agent can either return results synchronously or asynchronously.
#### (batch or streaming) base_query_builder: 
The BatchBaseQueryBuilder and StreamingBaseQueryBuilder are the interface to convert an indicator definition 
to actual queries that can then run against the database or data warehouse. 

It also controls the logic for pulling schemas from the data source. 

#### base_sink: 
The BaseSink provides the interface to write out the metric timeseries to a data source. 
The implemented `sink/lariat_sink` is used by default, and is used in the majority of cases. 

Note: Sink definitions are read from the config file, 
and adding a new sink requires tweaking the code in the base_agent constructor.

## Implementing an Agent

Add the data source to the `lariat_agents/agent/` path. 
To this path, you will expect to include concrete implementations of the base_agent and 
of the query builder. 

The additional requirement is to have a Dockerfile, executor and requirements.txt file that serves as the entry point 
for the agent to actually run on the cloud.

An agent can be in either synchronous or asynchronous mode. In asynchronous mode the agent doesn't wait for the results 
of the query once the query builder builds and runs the query. In synchronous mode the results are available immediately. 

For the asynchronous mode, the `map_action_to_function` generally includes an action that receives a signal 
indicating that a particular query result is now available to be written out to a sink.