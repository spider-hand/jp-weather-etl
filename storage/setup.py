"""Create the buckets required by the pipeline."""

from storage import BUCKETS, create_s3_client


def setup_storage() -> None:
    client = create_s3_client()
    existing = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}

    for bucket in BUCKETS:
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)

    print(f"RustFS buckets are ready: {', '.join(BUCKETS)}")


if __name__ == "__main__":
    setup_storage()
