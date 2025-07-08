# Reviews File Validation and Splitter Service

## Table of Contents

* [About the Application](#about-the-application)
* [Features](#features)
* [Getting Started](#getting-started)
    * [Prerequisites](#prerequisites)
    * [Local Development](#local-development)
    * [Running with Docker Locally](#running-with-docker-locally)
* [Configuration](#configuration)
* [Deployment to AWS ECS](#deployment-to-aws-ecs)
    * [ECS Task Definition](#ecs-task-definition)
    * [IAM Permissions](#iam-permissions)
    * [Deployment Steps (Manual/CI/CD)](#deployment-steps-manualcicd)
* [S3 File Processing](#s3-file-processing)
* [API Endpoints (if applicable)](#api-endpoints-if-applicable)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)

---

## About the Application

This Spring Boot application is designed to efficiently read and process files from an Amazon S3 bucket. It's implemented with scalability in mind, leveraging **Docker** for containerization and **AWS ECS** for orchestrated deployment. 

The primary function of this service involves fetching review files uploaded from the upload folder in S3, perform basic checks on the JSON structure validation, based on the size, split and upload the chunks to another bucket for further processing. Once the file is split, the original file is moved to an archive bucket for auditing.

---

## Features

* **Spring Boot**: Robust and easy-to-develop backend framework.
* **S3 Integration**: Seamlessly reads files from designated S3 buckets via SQS.
* **Pull rather than Push**: Instead of periodic pulls, the service employs a listener which processes files on arrival
* **Docker Support**: Containerized for consistent local development and cloud deployment.
* **ECS Deployment Ready**: Optimized for deployment within an AWS Elastic Container Service (ECS) cluster.
* **Configurable**: Easy to configure S3 bucket names, file paths, and processing parameters.

---

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

* **Java Development Kit (JDK) 17+**
* **Gradle**
* **Docker Desktop** (or Docker Engine)
* **AWS CLI** (for ECS deployment and S3 access)
* **An AWS Account** with necessary permissions (S3, ECS, IAM) - A user created to access this particular VPC is also created. 

### Local Development

1.  **Clone the repository:**

    ```bash
    git clone [https://github.com/sgbangalore34/review-splitter-service.git]
    cd review-splitter-service
    ```

2.  **Build the application:**

    ```bash
    .\gradlew bootjar
    ```

3.  **Run the application locally:**

    ```bash
    .\gradlew bootRun or from IDE Run Configuration
    ```

    The application will typically start on `http://localhost:8080` (or another port if configured).

### Running with Docker Locally

1.  **Build the Docker image:**

    ```bash
    docker build -t review-splitter-service .
    ```

2.  **Run the Docker container locally:**

    ```bash
    docker run -p 8080:8080 review-splitter-service:latest
    ```
    *Replace `YOUR_LOCAL_AWS_ACCESS_KEY`, `YOUR_LOCAL_AWS_SECRET_KEY`, `your-aws-region`, and `your-local-s3-bucket` with your actual local AWS credentials and a bucket you have access to.*
 
3. **Push the image to Amazon ECR:**

    ```bash
    docker tag review-splitter-service:latest `ACCOUNT_ID`.dkr.ecr.`your-aws-region`.amazonaws.com/review-splitter-service:latest
   
   docker push 008984192853.dkr.ecr.eu-north-1.amazonaws.com/review-splitter-service:latest
    ```
    *Replace `ACCOUNT_ID` and `your-aws-region` with your actual local AWS credentials and a bucket you have access to.*
---

## Configuration

Application properties are managed via `application.properties` (or `application.yml`). Key configurations include:

* `aws.region`=eu-north-1
* `aws.accessKeyId`=Provided access key
* `aws.secretKey`=Provided secret
* `aws.s3.source-bucket-name`=sg-reviews-uploads-bucket
* `aws.s3.destination-bucket-name`=sg-reviews-reducer-bucket
* `aws.s3.archive-bucket-name`=sg-reviews-archive

* `split-file-lines-per-chunk`=Desired number

---

## Deployment to AWS ECS

This application is designed for seamless deployment to an AWS ECS cluster. Once the docker image is pushed according to the push command above create a task definition and associate the service within

### ECS Task Definition

You will need an **ECS Task Definition** that specifies:

* **Container Image**: `review-splitter-service:latest` (or the specific tag you're deploying).
* **Port Mappings**: Map container port `8080` to a host port (if using EC2 launch type) or rely on dynamic port mapping with Fargate.
* **Environment Variables**: Pass necessary AWS credentials (via IAM Roles), S3 bucket name, and other configurations.
* **CPU and Memory**: Allocate appropriate resources.
* **Logging**: Configure CloudWatch Logs for application logs.