# ForHireVehicles
FetchFHV.py - is to download the file to your local drive
lambda_function.py - is lambda code to fetch the API data, save to /tmp, and then upload to S3 bucket
DeployLambda.sh - is the infrastructure code to deploy lambda function in AWS, with roles, policies added to S3 bucket, Lambda function etc.

To Read data,
created Athena results folder
created AWS Catalog Crawler-on demand, AWS Glue Database
Ran the Glue Crawler
Ran the Athena SQL editor SQL.