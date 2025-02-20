# Modern ELT Pipeline with Clouds

*Patcharanat P.*

## Prerequisites
- Program Installed
    - Python
    - Docker
    - Terraform
    - gcloud CLI
    - aws CLI
- Sensitive Information pre-created
    ```bash
    # .env
    # TODO: Get MongoDB Connection URI From Web UI
    MONGO_URI="mongodb+srv://....mongodb.net"
    GCP__PROJECT="your-project-name"
    ```
    ```bash
    # .terraform/aws/terraform.tfvars
    profile = "<profile-name>"
    ```
    ```bash
    # .terraform/gcp/terraform.tfvars
    service_account_email = "<service-account-name>@<gcp-project-name>.iam.gserviceaccount.com"
    project_id            = "<gcp-project-name>"
    location              = "<region>"
    ```

## Setting up Environment

### Initiating Docker Containers

```bash
make start

# make stop
```
Explanation
- I modified template to use Kafka with redpanda console (conduktor removed)
- At First, if we use original template from Airflow and redpanda (or conduktor), we will not be able to open redpanda console, due to duplicated port exposed, so changing port for redpanda is an only option.
    - Console
        - airflow WebUI: http://localhost:8080
        - kafka (redpanda) console: http://localhost:8085
- Redpanda implicitly use port 8080 to expose, can be changed by setting a specific environment variable for redpanda service, but it's unnecessary, since we can change port to be exposed in higher level in docker-compose.

*Disclaimer*
- Using Docker Compose is not appropriate for production environment.

References
- Airflow Docker Compose Template
    - [Official Airflow Docker Compose Template - Apache Airflow](https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml)
- Kafka Docker Compose Template
    - [Conduktor Kafka Docker Compose Template (Full Stack) - GitHub](https://github.com/conduktor/kafka-stack-docker-compose/blob/master/full-stack.yml)
    - [Redpanda Console Docker Compose Template - GitHub](https://github.com/redpanda-data/console/blob/master/docs/local/docker-compose.yaml)

### Cloud Authentication

```bash
# AWS
aws configure sso
# SSO session name: <session-name>
# SSO start URL: <retrieved from AWS Identity Center>
# SSO region: <your-sso-region>
# SSO registration scopes: <leave-blank>

aws configure list-profiles

aws sso login --profile <profile-name>
# Login via WebUI

# GCP
gcloud auth application-default login --impersonate-service-account <service-account-name>@<gcp-project-name>.iam.gserviceaccount.com
```

Explanation
- For me, AWS SSO method is like ADC method in GCP by using account A to act as another account B to grant permissions and be able to interact with cloud resources with permissions of account B.
- Both AWS SSO and GCP ADC are only recommended for local development and make long-lived credentials lesser to be concerned by utilizing global credentials in a local machine with short-lived credentials concept.

References
- Amazon Authentication
    - [Configuration and credential file settings in the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
- GCP Authentication Detail from Another Project
    - [GCP ADC for Terraform - Ecommerce-Invoice-End-to-end - GitHub](https://github.com/patcha-ranat/Ecommerce-Invoice-End-to-end?tab=readme-ov-file#222-gcp-adc-for-terraform)

### Initiating Cloud Resources

```bash
# GCP
# .terrraform/gcp

# AWS
# .terrraform/aws

terraform init
# terraform validate
# terraform fmt
terraform plan
terraform apply
terraform destroy
```

References
- Terraform AWS
    - [AWS Provider (Authentication related) for SSO, please refer to shared credentials - Terraform](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
    - [Terraform DynamoDB resource - Terraform ](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/dynamodb_table)
    - [Using Terraform with AWS SSO accounts - AWS](https://repost.aws/questions/QUgd8bMJKIRRqgSof0ksVKbA/using-terraform-with-aws-sso-accounts)
    - [IAM vs IAM Identity Center - Reddit](https://www.reddit.com/r/aws/comments/14j4wmn/iam_or_iam_identity_center/)
### Initiating Data

```bash
make venv

source pyenv/Scripts/activate

make install

./tools/data_init/kaggle_wrapper.sh

python tools/data_init/converter_main.py
```

Then please refer to [input_example.sh](./tools/data_init/scripts/input_example.sh) for initiating loading data to different targets

References
- Firestore Python API
    - [Add and update data - GCP](https://cloud.google.com/firestore/docs/manage-data/add-data#pythonasync_6)
    - [Delete documents and fields - GCP](https://cloud.google.com/firestore/docs/manage-data/delete-data)
- DynamoDB Python API
    - [Programming Amazon DynamoDB with Python and Boto3 - AWS](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/programming-with-python.html)
    - [boto3 Documentation Amazon - DynamoDB - AWS](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html)
    - [Using boto3 Session to select profile - Stack Overflow](https://stackoverflow.com/questions/33378422/how-to-choose-an-aws-profile-when-using-boto3-to-connect-to-cloudfront)
    - [Avoid unexpected behavior while inserting to dynamodb table by always explicitly define Item parameter - Stack Overflow](https://stackoverflow.com/questions/63615560/boto3-dynamodb-put-item-error-only-accepts-keyword-arguments)
- Kafka Confluent (Python API Client)
    - [confluent-kafka-python - GitHub](https://github.com/confluentinc/confluent-kafka-python/tree/master)
    - [Example of JSON producer - GitHub](https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/json_producer.py)
    - [confluent-kafka api docs - docs.io](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)

*In progress . . .*