# Fast Presigned URL Integration

## Overview

This integration adds a high-performance presigned URL generator to signurlarity, achieving **9-13x speedup** over boto3 by using pure Python stdlib (no boto3 dependency for URL generation).

Created during the DiracX hackathon for Rucio performance optimization.

## What Changed

### New Files

- **`src/signurlarity/presigner.py`** - Pure stdlib AWS Signature V4 implementation
  - Uses only `hashlib`, `hmac`, and `datetime`
  - Supports AWS and custom S3 endpoints
  - Handles both path-style and virtual-hosted URLs

### Modified Files

- **`src/signurlarity/client.py`** - Updated to use the fast presigner
  - boto3-compatible API (drop-in replacement)
  - Automatic region extraction from endpoint URLs
  - Maps boto3 method names to HTTP methods

### Benchmark Files

- **`new_presigner.py`** - Standalone presigner for performance comparison
- **`tests/test_perf.py`** - Performance benchmark tests

## Performance Results

### With Network Simulation (Moto S3 Mock)
- **boto3**: 1.246 ms per operation
- **Custom presigner**: 0.094 ms per operation
- **Speedup**: 13.3x faster

### Benchmark Results (5000 operations)
- **macOS** (Python 3.12): 9.05x faster
- **Linux** (Python 3.11): 13.01x faster

## Installation

```bash
# Clone the repository
git clone https://github.com/alessio94/signurlarity.git
cd signurlarity

# Checkout the integration branch
git checkout claude/check-visibility-Z0wof

# Install with testing dependencies
pip install -e ".[testing]"
```

## Usage

### Basic Usage (boto3-compatible)

```python
from signurlarity import Client

# Create a client
client = Client(
    endpoint_url='https://s3.us-west-2.amazonaws.com',
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY'
)

# Generate a presigned URL for GET
url = client.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'mybucket', 'Key': 'myfile.txt'},
    ExpiresIn=3600  # 1 hour
)

# Generate a presigned URL for PUT
upload_url = client.generate_presigned_url(
    'put_object',
    Params={'Bucket': 'mybucket', 'Key': 'upload.txt'},
    ExpiresIn=300  # 5 minutes
)
```

### Supported Operations

- ✅ `get_object` (GET)
- ✅ `put_object` (PUT)
- ✅ `delete_object` (DELETE)
- ✅ `head_object` (HEAD)

### Supported Regions

All AWS regions are supported, including:
- `us-east-1`, `us-west-2`
- `eu-west-1`, `eu-central-1`
- `ap-southeast-1`, `ap-northeast-1`
- And all others

## Running Tests

### Functional Tests

Test that the integration works correctly:

```bash
# Test the integrated Client class
python -m pytest tests/test_client.py::test_generate_presigned_url -v
```

Expected output:
```
tests/test_client.py::test_generate_presigned_url PASSED [100%]
```

### Performance Benchmarks

Compare standalone presigner vs boto3:

```bash
# Run performance benchmark
python -m pytest -s tests/test_perf.py
```

Expected output:
```
============================================================
PRESIGNED URL (GET) BENCHMARK
============================================================
boto3 generate_presigned_url: 1.4589s for 5000 ops (3427 ops/s)
custom S3PresignedURLGenerator: 0.1612s for 5000 ops (31017 ops/s)
relative speed (custom vs boto3): 9.05x
✓ Custom implementation is 9.05x FASTER!
============================================================
```

### Run All Tests

```bash
# Run all client tests
python -m pytest tests/test_client.py -v

# Run all tests (including async)
python -m pytest tests/ -v
```

## Technical Details

### AWS Signature Version 4 Implementation

The presigner implements AWS SigV4 signing:
1. Creates canonical request with URI, query params, and headers
2. Generates string to sign with timestamp and credential scope
3. Derives signing key using HMAC-SHA256
4. Calculates signature and appends to URL

### Endpoint Handling

**AWS Endpoints** (virtual-hosted style):
```
https://bucket.s3.amazonaws.com/key
https://bucket.s3.us-west-2.amazonaws.com/key
```

**Custom Endpoints** (path-style for testing):
```
http://localhost:27132/bucket/key
```

### Region Extraction

The client automatically extracts regions from endpoint URLs:
```python
s3.amazonaws.com              → us-east-1
s3.us-west-2.amazonaws.com    → us-west-2
localhost:27132               → us-east-1 (default)
```

## What's Not Implemented

- ❌ `generate_presigned_post` - Different feature (POST policy)
- ❌ `head_bucket` / `create_bucket` - Not needed for presigning
- ❌ `head_object` / `list_objects` - Not needed for presigning

These focus on presigned URL generation only.

## Benefits for Rucio

1. **9-13x faster** URL generation
2. **Zero boto3 overhead** for presigning operations
3. **Lower CPU usage** at scale
4. **Smaller memory footprint**
5. **Fewer dependencies** to maintain

Especially beneficial in high-throughput scenarios where thousands of presigned URLs are generated per second.

## File Structure

```
signurlarity/
├── src/signurlarity/
│   ├── presigner.py          # Fast presigner implementation
│   ├── client.py              # boto3-compatible Client class
│   ├── exceptions.py          # Error classes
│   └── __init__.py
├── tests/
│   ├── test_client.py         # Integration tests
│   └── test_perf.py           # Performance benchmarks
├── new_presigner.py           # Standalone benchmark version
└── INTEGRATION_README.md      # This file
```

## Contributing

This integration was created during the DiracX hackathon. To contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest tests/`
5. Submit a pull request

## Questions?

For questions about this integration, contact:
- **Repository**: https://github.com/alessio94/signurlarity
- **Branch**: `claude/check-visibility-Z0wof`

## License

See [LICENSE](LICENSE) file in the repository.
