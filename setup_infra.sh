# Removed some info and parts relating to AWS

# Check if docker is running;
if ! docker info >/dev/null 2>&1; then
    echo "Docker does not seem to be running, run it first and retry"
    exit 1
fi

AWS_ID=$(aws sts get-caller-identity --query Account --output text | cat)
AWS_REGION=$(aws configure get region)

SERVICE_NAME=
IAM_ROLE_NAME=

REDSHIFT_USER=
REDSHIFT_PASSWORD=
REDSHIFT_PORT=
EMR_NODE_TYPE=m4.xlarge

echo "Spinning up local Airflow infrastructure"
rm -rf logs
mkdir logs
rm -rf temp
mkdir temp
docker compose up airflow-init
docker compose up -d
echo "Sleeping 5 Minutes to let Airflow containers reach a healthy state"
sleep 300

echo "Creating an AWS EMR Cluster named "$SERVICE_NAME""
aws emr create-default-roles >> setup.log
aws emr create-cluster --applications Name=Hadoop Name=Spark --release-label emr-6.2.0 --name $SERVICE_NAME --scale-down-behavior TERMINATE_AT_TASK_COMPLETION  --service-role EMR_DefaultRole --instance-groups '[
    {
        "InstanceCount": 1,
        "EbsConfiguration": {
            "EbsBlockDeviceConfigs": [
                {
                    "VolumeSpecification": {
                        "SizeInGB": 32,
                        "VolumeType": "gp2"
                    },
                    "VolumesPerInstance": 2
                }
            ]
        },
        "InstanceGroupType": "MASTER",
        "InstanceType": "'$EMR_NODE_TYPE'",
        "Name": "Master - 1"
    },
    {
        "InstanceCount": 2,
        "BidPrice": "OnDemandPrice",
        "EbsConfiguration": {
            "EbsBlockDeviceConfigs": [
                {
                    "VolumeSpecification": {
                        "SizeInGB": 32,
                        "VolumeType": "gp2"
                    },
                    "VolumesPerInstance": 2
                }
            ]
        },
        "InstanceGroupType": "CORE",
        "InstanceType": "'$EMR_NODE_TYPE'",
        "Name": "Core - 2"
    }
        ]' >> setup.log

echo '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}' > ./trust-policy.json


echo "Creating an AWS Redshift Cluster named "$SERVICE_NAME""
aws redshift create-cluster --cluster-identifier $SERVICE_NAME --node-type dc2.large --master-username $REDSHIFT_USER --master-user-password $REDSHIFT_PASSWORD --cluster-type single-node --publicly-accessible --iam-roles "arn:aws:iam::"$AWS_ID":role/"$IAM_ROLE_NAME"" >> setup.log

while :
do
   echo "Waiting for Redshift cluster "$SERVICE_NAME" to start, sleeping for 60s before next check"
   sleep 60
   REDSHIFT_CLUSTER_STATUS=$(aws redshift describe-clusters --cluster-identifier $SERVICE_NAME --query 'Clusters[0].ClusterStatus' --output text)
   if [[ "$REDSHIFT_CLUSTER_STATUS" == "available" ]]
   then
	break
   fi
done

REDSHIFT_HOST=$(aws redshift describe-clusters --cluster-identifier $SERVICE_NAME --query 'Clusters[0].Endpoint.Address' --output text)

psql -f ./redshift_setup.sql postgres://$REDSHIFT_USER:$REDSHIFT_PASSWORD@$REDSHIFT_HOST:$REDSHIFT_PORT/dev
rm ./redshift_setup.sql

echo "adding redshift connections to Airflow connection param"
docker exec -d beginner_de_project_airflow-webserver_1 airflow connections add 'redshift' --conn-type 'Postgres' --conn-login $REDSHIFT_USER --conn-password $REDSHIFT_PASSWORD --conn-host $REDSHIFT_HOST --conn-port $REDSHIFT_PORT --conn-schema 'dev'
docker exec -d beginner_de_project_airflow-webserver_1 airflow connections add 'postgres_default' --conn-type 'Postgres' --conn-login 'airflow' --conn-password 'airflow' --conn-host 'localhost' --conn-port 5432 --conn-schema 'airflow'

echo "adding S3 bucket name to Airflow variables"
docker exec -d beginner_de_project_airflow-webserver_1 airflow variables set BUCKET $1

echo "adding EMR ID to Airflow variables"
EMR_CLUSTER_ID=$(aws emr list-clusters --active --query 'Clusters[?Name==`'$SERVICE_NAME'`].Id' --output text)
docker exec -d beginner_de_project_airflow-webserver_1 airflow variables set EMR_ID $EMR_CLUSTER_ID

echo "Setting up AWS access for Airflow workers"
AWS_ID=$(aws configure get aws_access_key_id)
AWS_SECRET_KEY=$(aws configure get aws_secret_access_key)
AWS_REGION=$(aws configure get region)
docker exec -d beginner_de_project_airflow-webserver_1 airflow connections add 'aws_default' --conn-type 'aws' --conn-login $AWS_ID --conn-password $AWS_SECRET_KEY --conn-extra '{"region_name":"'$AWS_REGION'"}'

echo "Finished setup."
