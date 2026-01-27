#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print AWS env vars after loading .env.")
    parser.add_argument("--env-file", help="Optional env file to load (e.g., .env).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file:
        load_dotenv(args.env_file, override=False)
    elif Path(".env").exists():
        load_dotenv(override=False)

    keys = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ]

    print("Loaded AWS env vars:")
    for key in keys:
        value = os.getenv(key, "")
        masked = value if len(value) <= 4 else f"{value[:2]}***{value[-2:]}"
        print(f"- {key}={masked if value else ''}")


if __name__ == "__main__":
    main()
