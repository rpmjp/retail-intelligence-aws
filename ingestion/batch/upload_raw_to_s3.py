"""Upload Online Retail II dataset to the S3 raw zone."""

import sys
from pathlib import Path

import boto3
import pandas as pd

RAW_FILE = Path("data/raw/online_retail_II.xlsx")
BUCKET = "retail-intelligence-raw-606493606327"
S3_KEY = "online_retail/online_retail_II.csv"


def load_dataset(path: Path) -> pd.DataFrame:
    """Load both sheets of the Excel file into one DataFrame."""
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(1)

    print(f"Reading {path} (both sheets)...")
    sheets = pd.read_excel(path, sheet_name=None)  # dict of all sheets
    df = pd.concat(sheets.values(), ignore_index=True)
    print(f"Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Write DataFrame to a local temp CSV and upload to S3."""
    tmp_path = Path("data/raw/_combined.csv")
    print(f"Writing temp CSV: {tmp_path}")
    df.to_csv(tmp_path, index=False)

    print(f"Uploading to s3://{bucket}/{key} ...")
    s3 = boto3.client("s3")
    s3.upload_file(str(tmp_path), bucket, key)

    tmp_path.unlink()  # clean up temp file
    print("Upload complete.")


def main() -> None:
    df = load_dataset(RAW_FILE)
    upload_to_s3(df, BUCKET, S3_KEY)


if __name__ == "__main__":
    main()
