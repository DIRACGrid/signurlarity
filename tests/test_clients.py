from __future__ import annotations

import logging
import tempfile

import botocore
import botocore.errorfactory
import httpx
import pytest

from signurlarity.exceptions import NoSuchBucketError, PresignError

from .conftest import (
    BUCKET_NAME as BASE_BUCKET_NAME,
)
from .conftest import (
    CHECKSUM_ALGORITHM,
    b16_to_b64,
    random_file,
)
from .conftest import (
    MISSING_BUCKET_NAME as BASE_MISSING_BUCKET_NAME,
)
from .conftest import (
    OTHER_BUCKET_NAME as BASE_OTHER_BUCKET_NAME,
)

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)

# Bucket names for sync tests
BUCKET_NAME = f"{BASE_BUCKET_NAME}"
OTHER_BUCKET_NAME = f"{BASE_OTHER_BUCKET_NAME}"
MISSING_BUCKET_NAME = f"{BASE_MISSING_BUCKET_NAME}"

# Bucket names for async tests (different to avoid conflicts)
BUCKET_NAME_AIO = f"{BASE_BUCKET_NAME}-aio"
OTHER_BUCKET_NAME_AIO = f"{BASE_OTHER_BUCKET_NAME}-aio"
MISSING_BUCKET_NAME_AIO = f"{BASE_MISSING_BUCKET_NAME}-aio"


# =============================================================================
# SYNC TESTS
# =============================================================================


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
    file_content = b"test content for head_object"
    key = "test_file.txt"
    boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME, Key=key, ContentType="text/plain"
    )
    response = light_client.head_object(Bucket=BUCKET_NAME, Key=key)
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
    with pytest.raises(PresignError):
        light_client.head_object(Bucket=BUCKET_NAME, Key="nonexistent_key.txt")


def test_head_object_missing_key_param(s3_clients):
    """Test that head_object raises PresignError when Key parameter is missing."""
    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.head_object(Bucket=BUCKET_NAME, Key="")


def test_head_object_missing_bucket_param(s3_clients):
    """Test that head_object raises PresignError when Bucket parameter is missing."""
    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.head_object(Bucket="", Key="some_key.txt")


def test_generate_presigned_post(s3_clients):
    """Upload files using post presigned URLs."""
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
        Bucket=BUCKET_NAME, Key=key, Fields=fields, Conditions=conditions, ExpiresIn=60
    )
    with httpx.Client() as client:
        r = client.post(
            upload_info["url"], data=upload_info["fields"], files={"file": file_content}
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
        Bucket=BUCKET_NAME, Key=key, Body=file_content, ContentType="text/plain"
    )
    assert "ETag" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
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
    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.put_object(Bucket="", Key="key.txt", Body=b"data")


def test_put_object_missing_key(s3_clients):
    """Test that put_object raises PresignError when Key is missing."""
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
        Bucket=BUCKET_NAME, Key=dst_key, CopySource=f"{BUCKET_NAME}/{src_key}"
    )
    assert "CopyObjectResult" in response
    assert "ETag" in response["CopyObjectResult"]
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
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
    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.copy_object(
            Bucket="", Key="dst.txt", CopySource=f"{BUCKET_NAME}/src.txt"
        )


def test_copy_object_missing_copy_source(s3_clients):
    """Test that copy_object raises PresignError when CopySource is missing."""
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
        Filename=str(local_file), Bucket=BUCKET_NAME, Key=key
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
            Filename="/nonexistent/path/file.txt", Bucket=BUCKET_NAME, Key="key.txt"
        )


def test_delete_objects(s3_clients):
    """Test that delete_objects deletes multiple objects."""
    boto_client, light_client = s3_clients
    keys = ["delete-test-1.txt", "delete-test-2.txt", "delete-test-3.txt"]
    for key in keys:
        boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)
    for key in keys:
        boto_client.head_object(Bucket=BUCKET_NAME, Key=key)
    response = light_client.delete_objects(
        Bucket=BUCKET_NAME, Delete={"Objects": [{"Key": k} for k in keys]}
    )
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Deleted" in response
    deleted_keys = {d["Key"] for d in response["Deleted"]}
    assert deleted_keys == set(keys)
    for key in keys:
        with pytest.raises(botocore.exceptions.ClientError):
            boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_delete_objects_quiet(s3_clients):
    """Test that delete_objects with Quiet=True returns no Deleted list."""
    boto_client, light_client = s3_clients
    keys = ["delete-quiet-1.txt", "delete-quiet-2.txt"]
    for key in keys:
        boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)
    response = light_client.delete_objects(
        Bucket=BUCKET_NAME,
        Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True},
    )
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Errors" not in response
    for key in keys:
        with pytest.raises(botocore.exceptions.ClientError):
            boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_delete_objects_missing_bucket(s3_clients):
    """Test that delete_objects raises PresignError for missing Bucket."""
    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.delete_objects(
            Bucket="", Delete={"Objects": [{"Key": "test.txt"}]}
        )


def test_delete_objects_missing_objects(s3_clients):
    """Test that delete_objects raises PresignError for missing Objects."""
    _boto_client, light_client = s3_clients
    with pytest.raises(PresignError):
        light_client.delete_objects(Bucket=BUCKET_NAME, Delete={})
    with pytest.raises(PresignError):
        light_client.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": []})


# =============================================================================
# ASYNC TESTS
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "s3_clients_aio",
    [pytest.param("moto_server", marks=pytest.mark.moto)],
    indirect=True,
)
async def test_create_bucket_aio(s3_clients_aio):
    boto_client, async_light_client = s3_clients_aio
    with pytest.raises(botocore.errorfactory.ClientError):
        await boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME_AIO)
    await async_light_client.create_bucket(Bucket=OTHER_BUCKET_NAME_AIO)
    await boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME_AIO)


@pytest.mark.asyncio
async def test_head_bucket_exists_aio(s3_clients_aio):
    """Test that head_bucket succeeds for an existing bucket (async)."""
    _boto_client, async_light_client = s3_clients_aio
    response = await async_light_client.head_bucket(Bucket=BUCKET_NAME_AIO)
    assert "BucketRegion" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.asyncio
async def test_head_bucket_not_found_aio(s3_clients_aio):
    """Test that head_bucket raises NoSuchBucketError for non-existent bucket (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(NoSuchBucketError):
        await async_light_client.head_bucket(Bucket=MISSING_BUCKET_NAME_AIO)


@pytest.mark.asyncio
async def test_head_object_exists_aio(s3_clients_aio):
    """Test that head_object succeeds for an existing object (async)."""
    boto_client, async_light_client = s3_clients_aio
    file_content = b"test content for head_object"
    key = "test_file_aio.txt"
    await boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME_AIO, Key=key, ContentType="text/plain"
    )
    response = await async_light_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)
    assert "ContentLength" in response
    assert "ETag" in response
    assert "LastModified" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["ContentLength"] == len(file_content)
    assert response["ContentType"] == "text/plain"


@pytest.mark.asyncio
async def test_head_object_not_found_aio(s3_clients_aio):
    """Test that head_object raises PresignError for non-existent object (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.head_object(
            Bucket=BUCKET_NAME_AIO, Key="nonexistent_key.txt"
        )


@pytest.mark.asyncio
async def test_head_object_missing_key_param_aio(s3_clients_aio):
    """Test that head_object raises PresignError when Key parameter is missing (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.head_object(Bucket=BUCKET_NAME_AIO, Key="")


@pytest.mark.asyncio
async def test_head_object_missing_bucket_param_aio(s3_clients_aio):
    """Test that head_object raises PresignError when Bucket parameter is missing (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.head_object(Bucket="", Key="some_key.txt")


@pytest.mark.asyncio
async def test_generate_presigned_post_aio(s3_clients_aio):
    """Upload files using post presigned URLs with async client."""
    boto_client, async_light_client = s3_clients_aio
    file_content, checksum = random_file(128)
    key = f"{checksum}_aio.dat"
    size = len(file_content)
    fields = {
        "x-amz-checksum-algorithm": CHECKSUM_ALGORITHM,
        f"x-amz-checksum-{CHECKSUM_ALGORITHM}": b16_to_b64(checksum),
    }
    conditions = [["content-length-range", size, size]] + [
        {k: v} for k, v in fields.items()
    ]
    upload_info = await async_light_client.generate_presigned_post(
        Bucket=BUCKET_NAME_AIO,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=60,
    )
    with httpx.Client() as client:
        r = client.post(
            upload_info["url"], data=upload_info["fields"], files={"file": file_content}
        )
    assert r.status_code in (200, 204), r.text
    await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)


@pytest.mark.asyncio
async def test_generate_presigned_url_aio(s3_clients_aio, caplog):
    """Get a pre-signed URL with our async client, upload with httpx, check it exists with boto."""
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")
    boto_client, async_light_client = s3_clients_aio
    file_content, checksum = random_file(128)
    key = f"{checksum}_aio.dat"
    response = await boto_client.put_object(
        Body=file_content,
        Bucket=BUCKET_NAME_AIO,
        Key=key,
        Metadata={"Checksum": checksum},
    )
    presigned_url = await async_light_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME_AIO, "Key": key},
        ExpiresIn=3600,
    )
    with tempfile.TemporaryFile(mode="w+b") as fh:
        with httpx.Client() as http_client:
            response = http_client.get(presigned_url)
            response.raise_for_status()
            for chunk in response.iter_bytes():
                fh.write(chunk)


@pytest.mark.asyncio
async def test_put_object_aio(s3_clients_aio):
    """Test that put_object uploads bytes to a bucket (async)."""
    boto_client, async_light_client = s3_clients_aio
    file_content = b"hello from put_object async"
    key = "put-object-test-aio.txt"
    response = await async_light_client.put_object(
        Bucket=BUCKET_NAME_AIO, Key=key, Body=file_content, ContentType="text/plain"
    )
    assert "ETag" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
    head = await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)
    assert head["ContentLength"] == len(file_content)


@pytest.mark.asyncio
async def test_put_object_with_metadata_aio(s3_clients_aio):
    """Test that put_object stores metadata on the object (async)."""
    boto_client, async_light_client = s3_clients_aio
    file_content = b"data with metadata async"
    key = "put-object-meta-test-aio.txt"
    await async_light_client.put_object(
        Bucket=BUCKET_NAME_AIO,
        Key=key,
        Body=file_content,
        Metadata={"author": "test", "version": "1"},
    )
    head = await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)
    assert head["Metadata"].get("author") == "test"
    assert head["Metadata"].get("version") == "1"


@pytest.mark.asyncio
async def test_put_object_missing_bucket_aio(s3_clients_aio):
    """Test that put_object raises PresignError when Bucket is missing (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.put_object(Bucket="", Key="key.txt", Body=b"data")


@pytest.mark.asyncio
async def test_put_object_missing_key_aio(s3_clients_aio):
    """Test that put_object raises PresignError when Key is missing (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.put_object(
            Bucket=BUCKET_NAME_AIO, Key="", Body=b"data"
        )


@pytest.mark.asyncio
async def test_list_objects_empty_aio(s3_clients_aio):
    """Test list_objects on a bucket with no matching prefix (async)."""
    _boto_client, async_light_client = s3_clients_aio
    response = await async_light_client.list_objects(
        Bucket=BUCKET_NAME_AIO, Prefix="list-objects-nonexistent-prefix/"
    )
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["Contents"] == []
    assert response["IsTruncated"] is False


@pytest.mark.asyncio
async def test_list_objects_aio(s3_clients_aio):
    """Test list_objects returns uploaded objects (async)."""
    boto_client, async_light_client = s3_clients_aio
    keys = ["list-test-aio/a.txt", "list-test-aio/b.txt", "list-test-aio/c.txt"]
    for key in keys:
        await boto_client.put_object(Body=b"data", Bucket=BUCKET_NAME_AIO, Key=key)
    response = await async_light_client.list_objects(
        Bucket=BUCKET_NAME_AIO, Prefix="list-test-aio/"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    returned_keys = {obj["Key"] for obj in response["Contents"]}
    assert returned_keys.issuperset(set(keys))
    for obj in response["Contents"]:
        assert "Key" in obj
        assert "ETag" in obj
        assert "Size" in obj
        assert "LastModified" in obj


@pytest.mark.asyncio
async def test_list_objects_with_delimiter_aio(s3_clients_aio):
    """Test list_objects with delimiter groups common prefixes (async)."""
    boto_client, async_light_client = s3_clients_aio
    keys = ["delim-test-aio/dir1/file.txt", "delim-test-aio/dir2/file.txt"]
    for key in keys:
        await boto_client.put_object(Body=b"data", Bucket=BUCKET_NAME_AIO, Key=key)
    response = await async_light_client.list_objects(
        Bucket=BUCKET_NAME_AIO, Prefix="delim-test-aio/", Delimiter="/"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "CommonPrefixes" in response
    prefixes = {cp["Prefix"] for cp in response["CommonPrefixes"]}
    assert "delim-test-aio/dir1/" in prefixes
    assert "delim-test-aio/dir2/" in prefixes


@pytest.mark.asyncio
async def test_list_objects_missing_bucket_aio(s3_clients_aio):
    """Test that list_objects raises PresignError when Bucket is missing (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.list_objects(Bucket="")


@pytest.mark.asyncio
async def test_copy_object_aio(s3_clients_aio):
    """Test that copy_object copies an object using a string CopySource (async)."""
    boto_client, async_light_client = s3_clients_aio
    src_key = "copy-src-aio.txt"
    dst_key = "copy-dst-aio.txt"
    await boto_client.put_object(
        Body=b"copy me async", Bucket=BUCKET_NAME_AIO, Key=src_key
    )
    response = await async_light_client.copy_object(
        Bucket=BUCKET_NAME_AIO, Key=dst_key, CopySource=f"{BUCKET_NAME_AIO}/{src_key}"
    )
    assert "CopyObjectResult" in response
    assert "ETag" in response["CopyObjectResult"]
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    head = await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=dst_key)
    assert head["ContentLength"] == len(b"copy me async")


@pytest.mark.asyncio
async def test_copy_object_dict_source_aio(s3_clients_aio):
    """Test that copy_object works with a dict CopySource (async)."""
    boto_client, async_light_client = s3_clients_aio
    src_key = "copy-src-dict-aio.txt"
    dst_key = "copy-dst-dict-aio.txt"
    await boto_client.put_object(
        Body=b"dict source async", Bucket=BUCKET_NAME_AIO, Key=src_key
    )
    response = await async_light_client.copy_object(
        Bucket=BUCKET_NAME_AIO,
        Key=dst_key,
        CopySource={"Bucket": BUCKET_NAME_AIO, "Key": src_key},
    )
    assert "CopyObjectResult" in response
    await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=dst_key)


@pytest.mark.asyncio
async def test_copy_object_missing_bucket_aio(s3_clients_aio):
    """Test that copy_object raises PresignError when Bucket is missing (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.copy_object(
            Bucket="", Key="dst.txt", CopySource=f"{BUCKET_NAME_AIO}/src.txt"
        )


@pytest.mark.asyncio
async def test_copy_object_missing_copy_source_aio(s3_clients_aio):
    """Test that copy_object raises PresignError when CopySource is missing (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.copy_object(
            Bucket=BUCKET_NAME_AIO, Key="dst.txt", CopySource=""
        )


@pytest.mark.asyncio
async def test_upload_file_aio(s3_clients_aio, tmp_path):
    """Test that upload_file uploads a local file to S3 (async)."""
    boto_client, async_light_client = s3_clients_aio
    content = b"async file content to upload"
    local_file = tmp_path / "upload_test_aio.txt"
    local_file.write_bytes(content)
    key = "upload-file-test-aio.txt"
    result = await async_light_client.upload_file(
        Filename=str(local_file), Bucket=BUCKET_NAME_AIO, Key=key
    )
    assert result is None
    head = await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)
    assert head["ContentLength"] == len(content)


@pytest.mark.asyncio
async def test_upload_file_with_extra_args_aio(s3_clients_aio, tmp_path):
    """Test that upload_file forwards ExtraArgs to put_object (async)."""
    boto_client, async_light_client = s3_clients_aio
    content = b"async pdf content"
    local_file = tmp_path / "report_aio.pdf"
    local_file.write_bytes(content)
    key = "upload-file-extra-args-aio.pdf"
    await async_light_client.upload_file(
        Filename=str(local_file),
        Bucket=BUCKET_NAME_AIO,
        Key=key,
        ExtraArgs={"ContentType": "application/pdf"},
    )
    head = await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)
    assert head["ContentType"] == "application/pdf"
    assert head["ContentLength"] == len(content)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "s3_clients_aio",
    [pytest.param("moto_server", marks=pytest.mark.moto)],
    indirect=True,
)
async def test_upload_file_with_acl_extra_args_aio(s3_clients_aio, tmp_path):
    """Test that upload_file applies ACL from ExtraArgs (moto only, async)."""
    boto_client, async_light_client = s3_clients_aio
    content = b"async acl content"
    local_file = tmp_path / "acl_aio.txt"
    local_file.write_bytes(content)
    key = "upload-file-acl-aio.txt"
    await async_light_client.upload_file(
        Filename=str(local_file),
        Bucket=BUCKET_NAME_AIO,
        Key=key,
        ExtraArgs={"ACL": "public-read"},
    )
    acl = await boto_client.get_object_acl(Bucket=BUCKET_NAME_AIO, Key=key)
    grants = acl.get("Grants", [])
    assert any(
        grant.get("Permission") == "READ"
        and grant.get("Grantee", {}).get("URI")
        == "http://acs.amazonaws.com/groups/global/AllUsers"
        for grant in grants
    )


@pytest.mark.asyncio
async def test_upload_file_missing_file_aio(s3_clients_aio):
    """Test that upload_file raises OSError for a non-existent file (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(OSError):
        await async_light_client.upload_file(
            Filename="/nonexistent/path/file.txt", Bucket=BUCKET_NAME_AIO, Key="key.txt"
        )


@pytest.mark.asyncio
async def test_delete_objects_aio(s3_clients_aio):
    """Test that delete_objects deletes multiple objects (async)."""
    boto_client, async_light_client = s3_clients_aio
    keys = ["delete-test-1.txt", "delete-test-2.txt", "delete-test-3.txt"]
    for key in keys:
        await boto_client.put_object(
            Body=b"test content", Bucket=BUCKET_NAME_AIO, Key=key
        )
    for key in keys:
        await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)
    response = await async_light_client.delete_objects(
        Bucket=BUCKET_NAME_AIO, Delete={"Objects": [{"Key": k} for k in keys]}
    )
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Deleted" in response
    deleted_keys = {d["Key"] for d in response["Deleted"]}
    assert deleted_keys == set(keys)
    for key in keys:
        with pytest.raises(botocore.errorfactory.ClientError):
            await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)


@pytest.mark.asyncio
async def test_delete_objects_quiet_aio(s3_clients_aio):
    """Test that delete_objects with Quiet=True returns no Deleted list (async)."""
    boto_client, async_light_client = s3_clients_aio
    keys = ["delete-quiet-1.txt", "delete-quiet-2.txt"]
    for key in keys:
        await boto_client.put_object(
            Body=b"test content", Bucket=BUCKET_NAME_AIO, Key=key
        )
    response = await async_light_client.delete_objects(
        Bucket=BUCKET_NAME_AIO,
        Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True},
    )
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Errors" not in response
    for key in keys:
        with pytest.raises(botocore.errorfactory.ClientError):
            await boto_client.head_object(Bucket=BUCKET_NAME_AIO, Key=key)


@pytest.mark.asyncio
async def test_delete_objects_missing_bucket_aio(s3_clients_aio):
    """Test that delete_objects raises PresignError for missing Bucket (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.delete_objects(
            Bucket="", Delete={"Objects": [{"Key": "test.txt"}]}
        )


@pytest.mark.asyncio
async def test_delete_objects_missing_objects_aio(s3_clients_aio):
    """Test that delete_objects raises PresignError for missing Objects (async)."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(PresignError):
        await async_light_client.delete_objects(Bucket=BUCKET_NAME_AIO, Delete={})
    with pytest.raises(PresignError):
        await async_light_client.delete_objects(
            Bucket=BUCKET_NAME_AIO, Delete={"Objects": []}
        )
