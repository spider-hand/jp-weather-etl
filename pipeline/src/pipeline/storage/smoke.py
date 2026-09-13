"""Verify access to the pipeline's RustFS buckets."""

from uuid import uuid4

from pipeline.storage import BUCKETS, create_s3_client


def smoke_check() -> None:
    client = create_s3_client()
    payload = b"rustfs smoke check"

    for bucket in BUCKETS:
        key = f"_smoke/{uuid4()}"
        client.put_object(Bucket=bucket, Key=key, Body=payload)
        try:
            stored = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            if stored != payload:
                raise RuntimeError(
                    f"Smoke check returned unexpected data from {bucket!r}"
                )
        finally:
            client.delete_object(Bucket=bucket, Key=key)

    print(f"RustFS smoke check passed for buckets: {', '.join(BUCKETS)}")


if __name__ == "__main__":
    smoke_check()
