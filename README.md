# Generate Jobs for Workflows in Databricks

This is a brief demo on how to generate your job based on a table defining the relationship between the tasks. The demo leverages the [Jobs API](https://docs.databricks.com/api/workspace/jobs) for creating and running jobs.

The demo uses [Databricks secrets](https://docs.databricks.com/security/secrets/index.html) for securely managing the PAT token required for accessing the REST API. It's also highly recommended to use Databricks secrets for the Databricks workspace URL.