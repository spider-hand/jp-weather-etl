"""Create the buckets required by the pipeline."""

import json

from jp_weather_etl.storage import BUCKETS, PROCESSED_BUCKET, create_s3_client

PROCESSED_READ_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{PROCESSED_BUCKET}/*",
        }
    ],
}

PROCESSED_CORS = {
    "CORSRules": [
        {
            "AllowedHeaders": ["*"],
            "AllowedMethods": ["GET", "HEAD"],
            "AllowedOrigins": ["*"],
        }
    ]
}


def setup_storage() -> None:
    client = create_s3_client()
    existing = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}

    for bucket in BUCKETS:
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)

    client.put_bucket_policy(
        Bucket=PROCESSED_BUCKET,
        Policy=json.dumps(PROCESSED_READ_POLICY),
    )
    client.put_bucket_cors(
        Bucket=PROCESSED_BUCKET,
        CORSConfiguration=PROCESSED_CORS,
    )

    print(f"RustFS buckets are ready: {', '.join(BUCKETS)}")


if __name__ == "__main__":
    setup_storage()
