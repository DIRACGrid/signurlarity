# SignURLarity

Lightweight library to presign URLs compatible with what boto does.

## Installation

```bash
pip install signurlarity
```

## tests

[installation pixi](https://pixi.sh/latest/advanced/installation/)

This will run functionnal tests.
It will spawn docker container to test against `rustfs`

```bash
pixi run unit-test # add any pytest option you want
```

Any `pytest` argument can be added

## pre-commit

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


## Perf tests

For a full performance comparison, run

```bash
pixi run perf-comparison
```

This will compare the results of `boto` and `signurlarity` against rustfs for python version 3.11, 3.12, 3.13 and 3.14, and generate `json` files in `/tmp/perf_test`

If you want to run it for a specific version only:

```bash
pixi run -e py314 benchmark --test-results-dir=/whatever/you/want
```

you can then display it with

```bash
pixi run -e py314 display-benchmark-comparison --test-results-dir=/whatever/you/want
```

## Connection Pooling

SignURLarity now uses connection pooling for better performance. Both the synchronous `Client` and asynchronous `AsyncClient` maintain a single `httpx.Client` or `httpx.AsyncClient` instance respectively, which is reused across all requests.

### Benefits

- **Better Performance**: Connections are reused instead of being created and destroyed for each request
- **Lower Overhead**: No TCP handshake or TLS negotiation overhead for subsequent requests
- **HTTP/2 Support**: Proper connection multiplexing for HTTP/2
- **Automatic Cleanup**: Context managers handle resource management automatically

### Usage

```python
# Synchronous client with context manager (recommended)
from signurlarity import Client

with Client(
    endpoint_url="https://s3.us-west-2.amazonaws.com",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
) as client:
    # All requests in this block use the same HTTP client
    url1 = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": "mybucket", "Key": "file1.txt"},
        ExpiresIn=3600,
    )
    url2 = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": "mybucket", "Key": "file2.txt"},
        ExpiresIn=3600,
    )
# HTTP client is automatically closed when exiting the context

# Async client with context manager (recommended)
from signurlarity.aio import AsyncClient

async with AsyncClient(
    endpoint_url="https://s3.us-west-2.amazonaws.com",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
) as client:
    # All requests in this block use the same HTTP client
    url1 = await client.generate_presigned_url(
        "get_object",
        Params={"Bucket": "mybucket", "Key": "file1.txt"},
        ExpiresIn=3600,
    )
    url2 = await client.generate_presigned_url(
        "get_object",
        Params={"Bucket": "mybucket", "Key": "file2.txt"},
        ExpiresIn=3600,
    )
# HTTP client is automatically closed when exiting the context
```

## Profiling tests

You can run profiling tests


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
