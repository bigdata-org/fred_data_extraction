# AWS Lambda Setup for Snowflake Integration

## Step 1: Create a Lambda Layer

1. **Navigate to the AWS Lambda Console.**
2. **Go to the 'Layers' section and click 'Create Layer'.**
3. **Provide a name for the layer and upload the layer package from your local system or an S3 bucket.**
4. **Select the runtime as Python 3.9 and architecture as x86_64.**
5. **Click 'Create' to finalize the layer creation.**

## Step 2: Create a Lambda Function

1. **Go to the AWS Lambda Console and click 'Create Function'.**
2. **Choose 'Author from scratch', provide a function name, and select Python 3.9 as the runtime.**
3. **Under 'Permissions', attach an execution role with necessary permissions.**
4. **After creation, go to the 'Layers' section and attach the previously created layer.**
5. **Upload your function code from your local system or an S3 bucket.**

## Step 3: Configure and Deploy the Lambda Function

1. **In the Lambda function console, configure the function settings, including environment variables if needed.**
2. **Click 'Deploy' to update the function with the uploaded code.**

## Step 4: Create an AWS API Gateway Endpoint

1. **Go to the AWS API Gateway Console and create a new REST API.**
2. **Create a new resource and method (GET) for the API.**
3. **Integrate the API with the Lambda function by selecting 'Lambda Function' as the integration type.**
4. **Deploy the API by creating a new stage (e.g., 'prod').**
5. **After deployment, note down the public endpoint URL generated for accessing the Lambda function.**

## Summary

This setup ensures a structured deployment of AWS Lambda for Snowflake integration using a dedicated layer. The function is accessible via a REST API endpoint, making it publicly available for API calls.

