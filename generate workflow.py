# Databricks notebook source
# MAGIC %md ## Define dependencies

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG db500west;
# MAGIC USE SCHEMA sandbox_sascha;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE wf_dependency (task STRING, depends_on STRING);
# MAGIC
# MAGIC INSERT INTO wf_dependency VALUES
# MAGIC   ('bronze/b1.py', NULL),
# MAGIC   ('bronze/b2.py', NULL),
# MAGIC   ('bronze/b3.py', NULL),
# MAGIC   ('silver/s1.py', 'bronze/b1.py'),
# MAGIC   ('silver/s2.py', 'bronze/b1.py,bronze/b2.py');

# COMMAND ----------

# MAGIC %md ## Generate Job Description

# COMMAND ----------

import json
import copy

# read in workflow
with open('workflow/workflow.json') as json_file:
  workflow = json.load(json_file)

# read in tasks
with open('workflow/task.json') as json_file:
  task = json.load(json_file)

# read in depend on
with open('workflow/depends_on.json') as json_file:
  depend_on = json.load(json_file)

dependencies = spark.read.table("wf_dependency").collect()

# print the collected data
for d in dependencies:
  ctask = copy.deepcopy(task)
  ctask['task_key'] = d.task.replace("/", "_").replace(".py", "")
  ctask['spark_python_task']['python_file'] = d.task
  if d.depends_on is not None:
    for d in d.depends_on.split(","):
      cdo = copy.deepcopy(depend_on)
      cdo['task_key'] = d.replace("/", "_").replace(".py", "")
      ctask['depends_on'].append(cdo)
  workflow['tasks'].append(ctask)

# update branch
print(json.dumps(workflow, indent=3))

# COMMAND ----------

# MAGIC %md ## Start job

# COMMAND ----------

import requests

# Set the API endpoint URL
url_run = "<URL>/api/2.1/jobs/runs/submit"
url_status = "<URL>/api/2.1/jobs/runs/get"

# Get token
token = dbutils.secrets.get(scope="demo_job", key="sveapitoken")

# Set the headers with the PAT token
headers = {
    "Authorization": "Bearer " + token
}

# Make the API call
response_run_job = requests.post(url_run, headers=headers, json=workflow)

print(json.dumps(response_run_job.json(), indent=3))

# COMMAND ----------

# MAGIC %md ## Wait for the job to finish

# COMMAND ----------

import time

# Wait for the job to finish
while (requests.get(url_status, headers=headers, json=response_run_job.json()).json()['state']['life_cycle_state'] == "RUNNING"):
  print("Job is still running")
  time.sleep(5)

final_status = requests.get(url_status, headers=headers, json=response_run_job.json()).json()['state']['result_state']
if (final_status != "SUCCESS"):
  raise Exception("Job run failed with status: " + final_status)

print(json.dumps(requests.get(url_status, headers=headers, json=response_run_job.json()).json(), indent=3))

# COMMAND ----------

# MAGIC %md ## Create Job

# COMMAND ----------

url_create = "<URL>/api/2.1/jobs/create"

# Make the API call
response_run_job = requests.post(url_create, headers=headers, json=workflow)

print(json.dumps(response_run_job.json(), indent=3))
