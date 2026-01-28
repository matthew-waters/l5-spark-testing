#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch EMR cluster from CloudFormation and submit a Spark step."
    )
    parser.add_argument("--template", required=True, help="Path to CloudFormation template.")
    parser.add_argument(
        "--params", required=True, help="Path to CloudFormation parameters JSON."
    )
    parser.add_argument("--stack-name", help="CloudFormation stack name.")
    parser.add_argument("--job-name", required=True, help="EMR step name.")
    parser.add_argument(
        "--region",
        help="AWS region (overrides AWS_REGION/AWS_DEFAULT_REGION if set).",
    )
    parser.add_argument(
        "--env-file",
        help="Optional env file to load (e.g., .env).",
    )
    parser.add_argument("--app-path", required=True, help="Path to the local Spark app.")
    parser.add_argument(
        "--app-args",
        nargs=argparse.REMAINDER,
        default=[],
        help="Arguments for the Spark application (use after --app-args).",
    )
    return parser.parse_args()


def load_template(template_path: str) -> str:
    return Path(template_path).read_text()


def load_params(params_path: str) -> List[Dict[str, str]]:
    raw = json.loads(Path(params_path).read_text())
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return [{"ParameterKey": k, "ParameterValue": str(v)} for k, v in raw.items()]
    raise ValueError("Unsupported params format. Use JSON list or object.")


def default_stack_name() -> str:
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"emr-test-stack-{ts}"


def describe_stack_status(cfn, stack_name: str) -> str:
    response = cfn.describe_stacks(StackName=stack_name)
    return response["Stacks"][0]["StackStatus"]


def print_stack_events(cfn, stack_name: str, max_events: int = 10) -> None:
    events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
    print("\nRecent stack events:")
    for event in events[:max_events]:
        ts = event["Timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        reason = event.get("ResourceStatusReason", "")
        print(
            f"- {ts} {event['LogicalResourceId']} "
            f"{event['ResourceStatus']} {reason}".strip()
        )


def prompt_delete_stack(cfn, stack_name: str) -> None:
    choice = input(f"\nDelete stack '{stack_name}'? [y/N]: ").strip().lower()
    if choice == "y":
        print("Deleting stack...")
        cfn.delete_stack(StackName=stack_name)
        cfn.get_waiter("stack_delete_complete").wait(StackName=stack_name)
        print("Stack deleted.")


def wait_for_stack(cfn, stack_name: str) -> None:
    print(f"Creating stack '{stack_name}'...")
    waiter = cfn.get_waiter("stack_create_complete")
    waiter.wait(StackName=stack_name)
    print("Stack creation complete.")


def get_stack_outputs(cfn, stack_name: str) -> Dict[str, str]:
    stacks = cfn.describe_stacks(StackName=stack_name)["Stacks"]
    return {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}


def get_stack_resource_id(cfn, stack_name: str, resource_type: str) -> Optional[str]:
    resources = cfn.list_stack_resources(StackName=stack_name)["StackResourceSummaries"]
    for resource in resources:
        if resource["ResourceType"] == resource_type:
            return resource["PhysicalResourceId"]
    return None


def get_cluster_id(cfn, stack_name: str) -> str:
    outputs = get_stack_outputs(cfn, stack_name)
    if "ClusterId" in outputs:
        return outputs["ClusterId"]
    cluster_id = get_stack_resource_id(cfn, stack_name, "AWS::EMR::Cluster")
    if cluster_id:
        return cluster_id
    raise RuntimeError("ClusterId not found in stack outputs or resources.")


def get_artifact_bucket(cfn, stack_name: str) -> str:
    outputs = get_stack_outputs(cfn, stack_name)
    if "ArtifactBucketName" in outputs:
        return outputs["ArtifactBucketName"]
    bucket = get_stack_resource_id(cfn, stack_name, "AWS::S3::Bucket")
    if bucket:
        return bucket
    raise RuntimeError("Artifact bucket not found in stack outputs or resources.")


def add_emr_step(emr, cluster_id: str, job_name: str, app_s3_uri: str, app_args: List[str]) -> str:
    step = {
        "Name": job_name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", app_s3_uri, *app_args],
        },
    }
    response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    return response["StepIds"][0]


def wait_for_step(emr, cluster_id: str, step_id: str) -> str:
    terminal = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}
    while True:
        response = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response["Step"]["Status"]["State"]
        print(f"Step status: {state}")
        if state in terminal:
            return state
        time.sleep(30)


def wait_for_cluster_ready(emr, cluster_id: str) -> None:
    terminal = {"TERMINATED", "TERMINATED_WITH_ERRORS"}
    while True:
        status = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
        print(f"Cluster status: {status}")
        if status == "WAITING":
            return
        if status in terminal:
            raise RuntimeError(f"Cluster entered terminal state: {status}")
        time.sleep(30)


def terminate_cluster(emr, cluster_id: Optional[str]) -> None:
    if not cluster_id:
        return
    emr.terminate_job_flows(JobFlowIds=[cluster_id])
    emr.get_waiter("cluster_terminated").wait(ClusterId=cluster_id)


def upload_app_to_s3(s3, bucket_name: str, app_path: Path, stack_name: str) -> str:
    app_key = f"apps/{stack_name}/{app_path.name}"
    s3.upload_file(str(app_path), bucket_name, app_key)
    return app_key


def chunked(items: Iterable[Dict[str, str]], size: int) -> Iterable[List[Dict[str, str]]]:
    batch: List[Dict[str, str]] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def delete_s3_prefix(s3, bucket_name: str, prefix: str) -> None:
    paginator = s3.get_paginator("list_objects_v2")
    keys: List[Dict[str, str]] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append({"Key": obj["Key"]})
    for batch in chunked(keys, 1000):
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": batch, "Quiet": True})


def cleanup_s3_artifacts(
    s3, bucket_name: Optional[str], app_key: Optional[str], output_prefix: Optional[str]
) -> None:
    if not bucket_name:
        return
    if app_key:
        s3.delete_object(Bucket=bucket_name, Key=app_key)
    if output_prefix:
        delete_s3_prefix(s3, bucket_name, output_prefix)


def main() -> int:
    args = parse_args()

    if args.env_file:
        load_dotenv(args.env_file, override=False)
    elif Path(".env").exists():
        load_dotenv(override=False)

    template_body = load_template(args.template)
    params = load_params(args.params)
    stack_name = args.stack_name or default_stack_name()

    session = boto3.session.Session(region_name=args.region)
    cfn = session.client("cloudformation")
    emr = session.client("emr")
    s3 = session.client("s3")

    def mask(value: str) -> str:
        if not value:
            return ""
        if len(value) <= 4:
            return value
        return f"{value[:2]}***{value[-2:]}"

    print("Using AWS credentials:")
    print(f"- AWS_ACCESS_KEY_ID={mask(os.getenv('AWS_ACCESS_KEY_ID', ''))}")
    print(f"- AWS_SECRET_ACCESS_KEY={mask(os.getenv('AWS_SECRET_ACCESS_KEY', ''))}")
    print(f"- AWS_SESSION_TOKEN={mask(os.getenv('AWS_SESSION_TOKEN', ''))}")
    print(f"- AWS_REGION={os.getenv('AWS_REGION', '')}")
    print(f"- AWS_DEFAULT_REGION={os.getenv('AWS_DEFAULT_REGION', '')}")

    app_path = Path(args.app_path)
    if not app_path.is_file():
        raise ValueError(f"App path not found: {app_path}")

    bucket_name: Optional[str] = None
    app_key: Optional[str] = None
    output_prefix: Optional[str] = None
    cluster_id: Optional[str] = None

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=params,
            Capabilities=["CAPABILITY_NAMED_IAM"],
        )
        wait_for_stack(cfn, stack_name)
        cluster_id = get_cluster_id(cfn, stack_name)
        print(f"Cluster ID: {cluster_id}")
        wait_for_cluster_ready(emr, cluster_id)

        bucket_name = get_artifact_bucket(cfn, stack_name)
        app_key = upload_app_to_s3(s3, bucket_name, app_path, stack_name)
        app_s3_uri = f"s3://{bucket_name}/{app_key}"
        output_prefix = f"outputs/{stack_name}/{args.job_name}/"
        output_s3_uri = f"s3://{bucket_name}/{output_prefix}"

        app_args = list(args.app_args)
        if "--output-s3" not in app_args:
            app_args.extend(["--output-s3", output_s3_uri])

        step_id = add_emr_step(emr, cluster_id, args.job_name, app_s3_uri, app_args)
        print(f"Submitted step: {step_id}")

        step_state = wait_for_step(emr, cluster_id, step_id)
        try:
            cleanup_s3_artifacts(s3, bucket_name, app_key, output_prefix)
        except Exception as cleanup_exc:
            print(f"Cleanup warning: {cleanup_exc}")
        try:
            terminate_cluster(emr, cluster_id)
        except Exception as terminate_exc:
            print(f"Terminate warning: {terminate_exc}")
        if step_state != "COMPLETED":
            print(f"Step failed with state: {step_state}")
            status = describe_stack_status(cfn, stack_name)
            print(f"Stack status: {status}")
            print_stack_events(cfn, stack_name)
            prompt_delete_stack(cfn, stack_name)
            return 1

        print("Step completed successfully.")
        return 0
    except (ClientError, BotoCoreError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}")
        try:
            try:
                cleanup_s3_artifacts(s3, bucket_name, app_key, output_prefix)
            except Exception as cleanup_exc:
                print(f"Cleanup warning: {cleanup_exc}")
            try:
                terminate_cluster(emr, cluster_id)
            except Exception as terminate_exc:
                print(f"Terminate warning: {terminate_exc}")
            status = describe_stack_status(cfn, stack_name)
            print(f"Stack status: {status}")
            print_stack_events(cfn, stack_name)
            prompt_delete_stack(cfn, stack_name)
        except Exception as inner_exc:
            print(f"Unable to inspect/delete stack: {inner_exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
