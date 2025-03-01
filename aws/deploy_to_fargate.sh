#!/bin/bash
# Script to deploy the producer container to AWS Fargate

# Exit on error
set -e

# Configuration
AWS_REGION="us-east-1"
ECR_REPOSITORY_NAME="weather-producer"
TASK_DEFINITION_FILE="fargate-task-def.json"
CLUSTER_NAME="weather-pipeline"
SERVICE_NAME="weather-producer-service"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "Deploying Weather Producer to AWS Fargate..."

# Ensure ECR repository exists
echo "Checking if ECR repository exists..."
aws ecr describe-repositories --repository-names $ECR_REPOSITORY_NAME --region $AWS_REGION || \
    aws ecr create-repository --repository-name $ECR_REPOSITORY_NAME --region $AWS_REGION

# Get ECR repository URI
ECR_REPOSITORY_URI=$(aws ecr describe-repositories --repository-names $ECR_REPOSITORY_NAME --region $AWS_REGION --query 'repositories[0].repositoryUri' --output text)
echo "ECR Repository URI: $ECR_REPOSITORY_URI"

# Update task definition with actual account ID and repository URI
echo "Updating task definition..."
sed -i "s/<your-account-id>/$AWS_ACCOUNT_ID/g" $TASK_DEFINITION_FILE
sed -i "s|<your-ecr-repository-uri>|$ECR_REPOSITORY_URI|g" $TASK_DEFINITION_FILE

# Log in to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REPOSITORY_URI

# Build and push the Docker image
echo "Building and pushing Docker image..."
cd ..
docker build -t $ECR_REPOSITORY_NAME .
docker tag $ECR_REPOSITORY_NAME:latest $ECR_REPOSITORY_URI:latest
docker push $ECR_REPOSITORY_URI:latest
cd aws

# Register the new task definition
echo "Registering task definition..."
TASK_DEFINITION_ARN=$(aws ecs register-task-definition --cli-input-json file://$TASK_DEFINITION_FILE --region $AWS_REGION --query 'taskDefinition.taskDefinitionArn' --output text)
echo "Task definition registered: $TASK_DEFINITION_ARN"

# Check if ECS cluster exists, create if it doesn't
echo "Checking if ECS cluster exists..."
aws ecs describe-clusters --clusters $CLUSTER_NAME --region $AWS_REGION --query 'clusters[0].clusterArn' --output text || \
    aws ecs create-cluster --cluster-name $CLUSTER_NAME --region $AWS_REGION

# Check if service exists
SERVICE_EXISTS=$(aws ecs list-services --cluster $CLUSTER_NAME --region $AWS_REGION | grep $SERVICE_NAME || echo "")

if [ -z "$SERVICE_EXISTS" ]; then
    # Create the service
    echo "Creating new service..."
    aws ecs create-service \
        --cluster $CLUSTER_NAME \
        --service-name $SERVICE_NAME \
        --task-definition $TASK_DEFINITION_ARN \
        --desired-count 1 \
        --launch-type FARGATE \
        --network-configuration "awsvpcConfiguration={subnets=[subnet-12345678],securityGroups=[sg-12345678],assignPublicIp=ENABLED}" \
        --region $AWS_REGION
else
    # Update the service with the new task definition
    echo "Updating existing service..."
    aws ecs update-service \
        --cluster $CLUSTER_NAME \
        --service $SERVICE_NAME \
        --task-definition $TASK_DEFINITION_ARN \
        --region $AWS_REGION
fi

echo "Deployment completed successfully!"
echo "Note: You need to update the subnet and security group IDs in this script with your actual AWS networking configuration." 