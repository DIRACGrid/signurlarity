# SignURLarity

Fast, lightweight S3 client focused on presigned URL generation. SignURLarity provides a boto3-compatible API with significantly better performance by avoiding the boto3 dependency overhead.

## Features

- **Fast presigned URL generation** (faster than boto3)
- **Async support** with `AsyncClient`
- **Connection pooling** for better performance
- **S3-compatible services** (AWS S3, MinIO, etc.)

## Performance

When creating a Pull Request, performance tests with respects to boto are ran and added as comment. Obviously, due to the nature of github Actions, the numbers may vary (a lot). Just as an example, here are the numbers of one of the later PR (the bigger the better). See below to run it yourself

| Test | 3.11 | 3.12 | 3.13 | 3.14 |
| --- | --- | --- | --- | --- |
| create_bucket_aio | 0.888 | 0.953 | 0.979 | 1.071 |
| create_bucket_aio_cm | 0.939 | 0.935 | 0.946 | 0.976 |
| create_bucket_sync | 1.145 | 1.194 | 1.142 | 1.198 |
| create_bucket_sync_cm | 1.141 | 1.144 | 1.171 | 1.173 |
| generate_presigned_post_aio | 8.516 | 8.722 | 8.606 | 8.804 |
| generate_presigned_post_aio_cm | 8.504 | 8.742 | 8.465 | 8.890 |
| generate_presigned_post_sync | 6.845 | 6.468 | 6.875 | 6.995 |
| generate_presigned_post_sync_cm | 6.713 | 6.465 | 6.638 | 6.768 |
| generate_presigned_url_aio | 15.597 | 15.490 | 15.075 | 17.026 |
| generate_presigned_url_aio_cm | 15.599 | 15.137 | 14.784 | 17.059 |
| generate_presigned_url_sync | 15.361 | 14.835 | 13.976 | 16.008 |
| generate_presigned_url_sync_cm | 14.739 | 14.806 | 14.014 | 16.203 |
| head_bucket_aio | 0.996 | 1.080 | 1.001 | 1.009 |
| head_bucket_aio_cm | 0.996 | 1.265 | 0.997 | 1.071 |
| head_bucket_sync | 1.468 | 1.522 | 1.456 | 1.522 |
| head_bucket_sync_cm | 1.211 | 1.147 | 1.505 | 1.507 |
| head_object_aio | 0.877 | 0.916 | 0.834 | 0.913 |
| head_object_aio_cm | 0.966 | 0.895 | 0.885 | 0.900 |
| head_object_sync | 1.516 | 1.443 | 0.898 | 1.584 |
| head_object_sync_cm | 1.493 | 1.547 | 1.511 | 1.470 |


## Installation

```bash
pip install signurlarity
```

## Quick Start

### Synchronous Client

```python
from signurlarity import Client

with Client(
    endpoint_url="https://s3.us-west-2.amazonaws.com",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
) as client:
    url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": "mybucket", "Key": "myfile.txt"},
        ExpiresIn=3600,
    )
```

### Async Client

```python
from signurlarity.aio import AsyncClient

async with AsyncClient(
    endpoint_url="https://s3.us-west-2.amazonaws.com",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
) as client:
    url = await client.generate_presigned_url(
        "get_object",
        Params={"Bucket": "mybucket", "Key": "myfile.txt"},
        ExpiresIn=3600,
    )
```

## Documentation

For detailed documentation including:
- Complete API reference
- Advanced usage examples
- Error handling
- Additional methods (presigned POST, head operations, etc.)

Please refer to the docstrings in the source code:
- [`Client`](src/signurlarity/client.py) - Synchronous client
- [`AsyncClient`](src/signurlarity/aio/client.py) - Asynchronous client
- [`S3Presigner`](src/signurlarity/presigner.py) - Low-level presigner

## Development

### Run tests

[installation pixi](https://pixi.sh/latest/advanced/installation/)

This will run functionnal tests.
It will spawn docker container to test against `rustfs`, `minio` and `moto`

```bash
pixi run unit-test # add any pytest option you want
```

Any `pytest` argument can be added

### pre-commit

SignURLarity uses [`pre-commit`](https://pre-commit.com/) to format code and check for issues.
The easiest way to use `pre-commit` is to run the following after cloning:

```bash
pixi run pre-commit install
```

This will result in pre-commit being ran automatically each time you run `git commit`.
If you want to explicitly run pre-commit you can use:

```bash
pixi run pre-commit # (1)!
pixi run pre-commit --all-files # (2)!
```

1. Runs `pre-commit` only for files which are uncommitted or which have been changed.
2. Runs `pre-commit` for all files even if you haven't changed them.


### Benchmark

For a full performance comparison, run

```bash
pixi run full-benchmark /whatever/outputdir
```

This will compare the results of `boto` and `signurlarity` against rustfs for python version 3.11, 3.12, 3.13 and 3.14, and generate `json` files in the output directoty

If you want to run it for a specific version only:

```bash
pixi run -e py314 benchmark --test-results-dir=/whatever/you/want
```

you can then display it with

```bash
pixi run -e py314 display-benchmark-comparison --test-results-dir=/whatever/you/want
```

### Profiling tests

A few profiling tests are available


```bash
pixi run -e py314 profile-test -s --test-results-dir=/whatever/you/want
```

This will generate `prof` [files](https://docs.python.org/3/library/profile.html)


You can convert it in svg and open it in your web browser like so



```shell
pixi shell -e py314
flameprof --format=log profile_generate_presigned_post/presigned_post.prof | flamegraph > profile_generate_presigned_post/presigned_post.svg
```

:

 ![Example results](profiling.svg)
