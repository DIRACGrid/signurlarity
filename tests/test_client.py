from __future__ import annotations

import logging
import tempfile

import botocore
import httpx
import pytest

from signurlarity.exceptions import NoSuchBucketError

from .conftest import (
    BUCKET_NAME,
    CHECKSUM_ALGORITHM,
    MISSING_BUCKET_NAME,
    OTHER_BUCKET_NAME,
    b16_to_b64,
    random_file,
)

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


def test_create_bucket(s3_clients):
    boto_client, light_client = s3_clients
    with pytest.raises(botocore.exceptions.ClientError):
        boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)
    light_client.create_bucket(Bucket=OTHER_BUCKET_NAME)
    boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)


def test_head_bucket_exists(s3_clients):
    """Test that head_bucket succeeds for an existing bucket."""
    _boto_client, light_client = s3_clients
    response = light_client.head_bucket(Bucket=BUCKET_NAME)

    # Verify response structure
    assert "BucketRegion" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_head_bucket_not_found(s3_clients):
    """Test that head_bucket raises NoSuchBucketError for non-existent bucket."""
    _boto_client, light_client = s3_clients
    with pytest.raises(NoSuchBucketError):
        light_client.head_bucket(Bucket=MISSING_BUCKET_NAME)


def test_head_object_exists(s3_clients):
    """Test that head_object succeeds for an existing object."""
    boto_client, light_client = s3_clients

    # First, create an object
    file_content = b"test content for head_object"
    key = "test_file.txt"
    boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME, Key=key, ContentType="text/plain"
    )

    # Now test head_object
    response = light_client.head_object(Bucket=BUCKET_NAME, Key=key)

    # Verify response structure
    assert "ContentLength" in response
    assert "ETag" in response
    assert "LastModified" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["ContentLength"] == len(file_content)
    assert response["ContentType"] == "text/plain"


def test_head_object_not_found(s3_clients):
    """Test that head_object raises PresignError for non-existent object."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.head_object(Bucket=BUCKET_NAME, Key="nonexistent_key.txt")


def test_head_object_missing_key_param(s3_clients):
    """Test that head_object raises PresignError when Key parameter is missing."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.head_object(Bucket=BUCKET_NAME, Key="")


def test_head_object_missing_bucket_param(s3_clients):
    """Test that head_object raises PresignError when Bucket parameter is missing."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.head_object(Bucket="", Key="some_key.txt")


# def test_head_bucket_missing_bucket_param(s3_clients):
#     """Test that head_bucket raises error when Bucket parameter is missing."""
#     _boto_client, light_client = s3_clients
#     with pytest.raises(Exception):  # PresignError or similar
#         light_client.head_bucket(Bucket="")


def test_generate_presigned_post(s3_clients):
    """Upload files using post presigned URLs.

    Get a pre-signed URL with our client, upload with httpx
    check it exists with boto.
    """
    boto_client, light_client = s3_clients

    file_content, checksum = random_file(128)
    key = f"{checksum}.dat"
    size = len(file_content)

    fields = {
        "x-amz-checksum-algorithm": CHECKSUM_ALGORITHM,
        f"x-amz-checksum-{CHECKSUM_ALGORITHM}": b16_to_b64(checksum),
    }
    conditions = [["content-length-range", size, size]] + [
        {k: v} for k, v in fields.items()
    ]

    upload_info = light_client.generate_presigned_post(
        Bucket=BUCKET_NAME,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=60,
    )

    with httpx.Client() as client:
        r = client.post(
            upload_info["url"],
            data=upload_info["fields"],
            files={"file": file_content},
        )

    assert r.status_code in (200, 204), r.text
    boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_generate_presigned_url(s3_clients, caplog):
    """Get a pre-signed URL with our client, upload with httpx, check it exists with boto."""
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    boto_client, light_client = s3_clients

    file_content, checksum = random_file(128)
    key = f"{checksum}.dat"

    response = boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME, Key=key, Metadata={"Checksum": checksum}
    )

    presigned_url = light_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=3600,
    )

    with tempfile.TemporaryFile(mode="w+b") as fh:
        with httpx.Client() as http_client:
            response = http_client.get(presigned_url)
            response.raise_for_status()
            for chunk in response.iter_bytes():
                fh.write(chunk)


def test_put_object(s3_clients):
    """Test that put_object uploads bytes to a bucket."""
    boto_client, light_client = s3_clients

    file_content = b"hello from put_object"
    key = "put-object-test.txt"

    response = light_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=file_content,
        ContentType="text/plain",
    )

    assert "ETag" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)

    # Verify via boto
    head = boto_client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head["ContentLength"] == len(file_content)


def test_put_object_with_metadata(s3_clients):
    """Test that put_object stores metadata on the object."""
    boto_client, light_client = s3_clients

    file_content = b"data with metadata"
    key = "put-object-meta-test.txt"

    light_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=file_content,
        Metadata={"author": "test", "version": "1"},
    )

    head = boto_client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head["Metadata"].get("author") == "test"
    assert head["Metadata"].get("version") == "1"


def test_put_object_missing_bucket(s3_clients):
    """Test that put_object raises PresignError when Bucket is missing."""
    from signurlarity.exceptions import PresignError

    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.put_object(Bucket="", Key="key.txt", Body=b"data")


def test_put_object_missing_key(s3_clients):
    """Test that put_object raises PresignError when Key is missing."""
    from signurlarity.exceptions import PresignError

    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.put_object(Bucket=BUCKET_NAME, Key="", Body=b"data")


def test_list_objects_empty(s3_clients):
    """Test list_objects on a bucket with no matching prefix."""
    _boto_client, light_client = s3_clients

    response = light_client.list_objects(
        Bucket=BUCKET_NAME, Prefix="list-objects-nonexistent-prefix/"
    )

    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["Contents"] == []
    assert response["IsTruncated"] is False


def test_list_objects(s3_clients):
    """Test list_objects returns uploaded objects."""
    boto_client, light_client = s3_clients

    keys = ["list-test/a.txt", "list-test/b.txt", "list-test/c.txt"]
    for key in keys:
        boto_client.put_object(Body=b"data", Bucket=BUCKET_NAME, Key=key)

    response = light_client.list_objects(Bucket=BUCKET_NAME, Prefix="list-test/")

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    returned_keys = {obj["Key"] for obj in response["Contents"]}
    assert returned_keys.issuperset(set(keys))
    for obj in response["Contents"]:
        assert "Key" in obj
        assert "ETag" in obj
        assert "Size" in obj
        assert "LastModified" in obj


def test_list_objects_with_delimiter(s3_clients):
    """Test list_objects with delimiter groups common prefixes."""
    boto_client, light_client = s3_clients

    keys = ["delim-test/dir1/file.txt", "delim-test/dir2/file.txt"]
    for key in keys:
        boto_client.put_object(Body=b"data", Bucket=BUCKET_NAME, Key=key)

    response = light_client.list_objects(
        Bucket=BUCKET_NAME, Prefix="delim-test/", Delimiter="/"
    )

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "CommonPrefixes" in response
    prefixes = {cp["Prefix"] for cp in response["CommonPrefixes"]}
    assert "delim-test/dir1/" in prefixes
    assert "delim-test/dir2/" in prefixes


def test_list_objects_missing_bucket(s3_clients):
    """Test that list_objects raises PresignError when Bucket is missing."""
    from signurlarity.exceptions import PresignError

    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.list_objects(Bucket="")


def test_copy_object(s3_clients):
    """Test that copy_object copies an object using a string CopySource."""
    boto_client, light_client = s3_clients

    src_key = "copy-src.txt"
    dst_key = "copy-dst.txt"
    boto_client.put_object(Body=b"copy me", Bucket=BUCKET_NAME, Key=src_key)

    response = light_client.copy_object(
        Bucket=BUCKET_NAME,
        Key=dst_key,
        CopySource=f"{BUCKET_NAME}/{src_key}",
    )

    assert "CopyObjectResult" in response
    assert "ETag" in response["CopyObjectResult"]
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Verify destination exists
    head = boto_client.head_object(Bucket=BUCKET_NAME, Key=dst_key)
    assert head["ContentLength"] == len(b"copy me")


def test_copy_object_dict_source(s3_clients):
    """Test that copy_object works with a dict CopySource."""
    boto_client, light_client = s3_clients

    src_key = "copy-src-dict.txt"
    dst_key = "copy-dst-dict.txt"
    boto_client.put_object(Body=b"dict source", Bucket=BUCKET_NAME, Key=src_key)

    response = light_client.copy_object(
        Bucket=BUCKET_NAME,
        Key=dst_key,
        CopySource={"Bucket": BUCKET_NAME, "Key": src_key},
    )

    assert "CopyObjectResult" in response
    boto_client.head_object(Bucket=BUCKET_NAME, Key=dst_key)


def test_copy_object_missing_bucket(s3_clients):
    """Test that copy_object raises PresignError when Bucket is missing."""
    from signurlarity.exceptions import PresignError

    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.copy_object(
            Bucket="", Key="dst.txt", CopySource=f"{BUCKET_NAME}/src.txt"
        )


def test_copy_object_missing_copy_source(s3_clients):
    """Test that copy_object raises PresignError when CopySource is missing."""
    from signurlarity.exceptions import PresignError

    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.copy_object(Bucket=BUCKET_NAME, Key="dst.txt", CopySource="")


def test_upload_file(s3_clients, tmp_path):
    """Test that upload_file uploads a local file to S3."""
    boto_client, light_client = s3_clients

    content = b"file content to upload"
    local_file = tmp_path / "upload_test.txt"
    local_file.write_bytes(content)

    key = "upload-file-test.txt"
    result = light_client.upload_file(
        Filename=str(local_file),
        Bucket=BUCKET_NAME,
        Key=key,
    )

    assert result is None

    head = boto_client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head["ContentLength"] == len(content)


def test_upload_file_with_extra_args(s3_clients, tmp_path):
    """Test that upload_file forwards ExtraArgs to put_object."""
    boto_client, light_client = s3_clients

    content = b"pdf content"
    local_file = tmp_path / "report.pdf"
    local_file.write_bytes(content)

    key = "upload-file-extra-args.pdf"
    light_client.upload_file(
        Filename=str(local_file),
        Bucket=BUCKET_NAME,
        Key=key,
        ExtraArgs={"ContentType": "application/pdf"},
    )

    head = boto_client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head["ContentType"] == "application/pdf"
    assert head["ContentLength"] == len(content)


# Only Moto exposes the correct ACL api
@pytest.mark.parametrize(
    "s3_clients",
    [pytest.param("moto_server", marks=pytest.mark.moto)],
    indirect=True,
)
def test_upload_file_with_acl_extra_args(s3_clients, tmp_path):
    """Test that upload_file applies ACL from ExtraArgs (moto only)."""
    boto_client, light_client = s3_clients

    content = b"acl content"
    local_file = tmp_path / "acl.txt"
    local_file.write_bytes(content)

    key = "upload-file-acl.txt"
    light_client.upload_file(
        Filename=str(local_file),
        Bucket=BUCKET_NAME,
        Key=key,
        ExtraArgs={"ACL": "public-read"},
    )

    acl = boto_client.get_object_acl(Bucket=BUCKET_NAME, Key=key)
    grants = acl.get("Grants", [])
    assert any(
        grant.get("Permission") == "READ"
        and grant.get("Grantee", {}).get("URI")
        == "http://acs.amazonaws.com/groups/global/AllUsers"
        for grant in grants
    ), grants


def test_upload_file_missing_file(s3_clients):
    """Test that upload_file raises OSError for a non-existent file."""
    _boto_client, light_client = s3_clients
    with pytest.raises(OSError):
        light_client.upload_file(
            Filename="/nonexistent/path/file.txt",
            Bucket=BUCKET_NAME,
            Key="key.txt",
        )


def test_delete_objects(s3_clients):
    """Test that delete_objects deletes multiple objects."""
    boto_client, light_client = s3_clients

    # Create some objects using boto
    keys = ["delete-test-1.txt", "delete-test-2.txt", "delete-test-3.txt"]
    for key in keys:
        boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)

    # Verify objects exist
    for key in keys:
        boto_client.head_object(Bucket=BUCKET_NAME, Key=key)

    # Delete objects using our client
    response = light_client.delete_objects(
        Bucket=BUCKET_NAME,
        Delete={"Objects": [{"Key": k} for k in keys]},
    )

    # Verify response structure
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Deleted" in response
    deleted_keys = {d["Key"] for d in response["Deleted"]}
    assert deleted_keys == set(keys)

    # Verify objects are actually gone
    for key in keys:
        with pytest.raises(botocore.exceptions.ClientError):
            boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_delete_objects_quiet(s3_clients):
    """Test that delete_objects with Quiet=True returns no Deleted list."""
    boto_client, light_client = s3_clients

    # Create objects
    keys = ["delete-quiet-1.txt", "delete-quiet-2.txt"]
    for key in keys:
        boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)

    # Delete with Quiet=True
    response = light_client.delete_objects(
        Bucket=BUCKET_NAME,
        Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True},
    )

    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    # In quiet mode, only errors are reported
    assert "Errors" not in response

    # Verify objects are actually gone
    for key in keys:
        with pytest.raises(botocore.exceptions.ClientError):
            boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_delete_objects_missing_bucket(s3_clients):
    """Test that delete_objects raises PresignError for missing Bucket."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.delete_objects(
            Bucket="",
            Delete={"Objects": [{"Key": "test.txt"}]},
        )


def test_delete_objects_missing_objects(s3_clients):
    """Test that delete_objects raises PresignError for missing Objects."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.delete_objects(Bucket=BUCKET_NAME, Delete={})

    with pytest.raises(PresignError):
        light_client.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={"Objects": []},
        )


def test_delete_bucket(s3_clients):
    """Test that delete_bucket correctly deleted."""
    boto_client, light_client = s3_clients

    # Create some objects using boto
    key = "delete-test-1.txt"
    boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)

    # Verify objects exist
    boto_client.head_object(Bucket=BUCKET_NAME, Key=key)

    # Delete objects before deleting bucket
    objects = light_client.list_objects(Bucket=BUCKET_NAME)
    light_client.delete_objects(
        Bucket=BUCKET_NAME,
        Delete={"Objects": [{"Key": obj["Key"]} for obj in objects["Contents"]]},
    )

    # Delete bucket using our async client
    response = light_client.delete_bucket(Bucket=BUCKET_NAME)
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
    assert "Contents" not in response


def test_delete_bucket_missing_bucket(s3_clients):
    """Test that delete_bucket raises PresignError for missing Bucket."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.delete_bucket(Bucket="")


def test_delete_bucket_not_empty(s3_clients):
    """Test that delete_bucket raises PresignError if Bucket is not empty."""
    boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    # Create some objects using boto
    key = "delete-test-1.txt"
    boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)

    # Verify objects exist
    boto_client.head_object(Bucket=BUCKET_NAME, Key=key)

    # Try to delete non empty bucket
    with pytest.raises(PresignError):
        light_client.delete_bucket(Bucket=BUCKET_NAME)


# @pytest.fixture()
# def fix_1():
#     print("entering fix 1")
#     yield "fix 1"
#     print("finishing fix 1")


# @pytest.fixture()
# def fix_2(worker_id):
#     print(f"entering fix 2 {worker_id}")
#     yield "fix 2"
#     print("finishing fix 2")


# @pytest.fixture(params=["fix_1", "fix_2"])
# def fix(request):
#     server_fixture = request.param
#     srv_fixt = request.getfixturevalue(server_fixture)
#     print("entering fix")
#     client_param = server_fixture
#     # breakpoint()
#     print(f"GOT {client_param}")
#     yield client_param

#     print("finishing fix")


# def test_my_fix(fix):
#     print(f"testing {fix}")
