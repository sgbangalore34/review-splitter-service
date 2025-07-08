EC2 Instances (eu-north-1 region)
# List all EC2 instances
aws ec2 describe-instances --region eu-north-1

# Start an EC2 instance
aws ec2 start-instances --instance-ids <instance-id> --region eu-north-1

# Stop an EC2 instance
aws ec2 stop-instances --instance-ids <instance-id> --region eu-north-1

# Get instance details
aws ec2 describe-instances --instance-ids <instance-id> --region eu-north-1

Run in CloudShell
S3 Buckets
# List all S3 buckets
aws s3 ls

# List objects in a specific bucket
aws s3 ls s3://<bucket-name>

# Create a new bucket
aws s3 mb s3://<new-bucket-name>

# Copy a file to a bucket
aws s3 cp <local-file> s3://<bucket-name>/<path>

# Remove a file from a bucket
aws s3 rm s3://<bucket-name>/<path>

Run in CloudShell
ECS Cluster (sg-zuzu-review-system-cluster in eu-north-1)
# List ECS clusters
aws ecs list-clusters --region eu-north-1

# Describe the specific cluster
aws ecs describe-clusters --clusters sg-zuzu-review-system-cluster --region eu-north-1

# List services in the cluster
aws ecs list-services --cluster sg-zuzu-review-system-cluster --region eu-north-1

# List tasks in the cluster
aws ecs list-tasks --cluster sg-zuzu-review-system-cluster --region eu-north-1

# Describe a specific service
aws ecs describe-services --cluster sg-zuzu-review-system-cluster --services <service-name> --region eu-north-1

# Update a service
aws ecs update-service --cluster sg-zuzu-review-system-cluster --service <service-name> --desired-count <count> --region eu-north-1
