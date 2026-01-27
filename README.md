# EMR CloudFormation Spark Test Repo

This repo provides a minimal setup for launching an EMR cluster via CloudFormation and
submitting a Spark job step. The example Spark app generates its own input data for
repeatable testing.

## Requirements
- Python 3.9+
- AWS credentials in environment variables:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN` (optional)
  - `AWS_REGION` or `AWS_DEFAULT_REGION`
- IAM roles in your account:
  - `EMR_DefaultRole`
  - `EMR_EC2_DefaultRole`

Install Python dependencies:
```
python -m pip install -r requirements.txt
```

Optional: copy `env.example` to `.env` and fill in your credentials/region, or
pass `--env-file path/to/env` to the launcher.

## Repository Layout
- `templates/` CloudFormation templates for EMR
- `templates/params/` Example parameter files for templates
- `spark_apps/` Example Spark apps (self-generated data)
- `scripts/` Launcher scripts

## Launch a Cluster and Run a Job
1. Update `templates/params/emr-cluster.params.json` with your `LogUri`, `KeyName`, and `SubnetId`.
2. Run the launcher:
```
python scripts/launch_emr.py \
  --template templates/emr-cluster.yaml \
  --params templates/params/emr-cluster.params.json \
  --job-name generated-wordcount \
  --region us-east-1 \
  --env-file .env \
  --app-path spark_apps/generated_wordcount.py \
  --app-args --rows 200000 --vocab-size 200 --words-per-row 8
```

The launcher uploads the app to the stack's S3 bucket, passes an output S3 location
to the app, and then removes the uploaded app and output data when the step ends.

If the stack or job fails, the script prints the stack status and recent events,
then offers to delete the stack.
