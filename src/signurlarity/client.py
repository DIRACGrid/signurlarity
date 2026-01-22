from typing import Any, Mapping
from urllib.parse import urlparse

from .presigner import S3Presigner
from .exceptions import PresignError


class Client:
    """S3 client for generating presigned URLs.

    This is a lightweight, boto3-compatible client that focuses on presigned URL
    generation without the boto3 dependency overhead.

    Args:
        endpoint_url: S3 endpoint URL (e.g., 'https://s3.amazonaws.com' or
                      'https://s3.us-west-2.amazonaws.com')
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key

    Example:
        >>> client = Client(
        ...     endpoint_url='https://s3.us-west-2.amazonaws.com',
        ...     aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
        ...     aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        ... )
        >>> url = client.generate_presigned_url(
        ...     'get_object',
        ...     Params={'Bucket': 'mybucket', 'Key': 'mykey'},
        ...     ExpiresIn=3600
        ... )
    """

    def __init__(
        self, endpoint_url: str, aws_access_key_id: str, aws_secret_access_key: str
    ):
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        # Extract region from endpoint URL
        self.region = self._extract_region(endpoint_url)

        # Initialize the presigner
        self._presigner = S3Presigner(
            access_key=aws_access_key_id,
            secret_key=aws_secret_access_key,
            region=self.region,
            endpoint_url=endpoint_url
        )

    def _extract_region(self, endpoint_url: str) -> str:
        """Extract AWS region from endpoint URL.

        Args:
            endpoint_url: S3 endpoint URL

        Returns:
            AWS region string (defaults to 'us-east-1')
        """
        parsed = urlparse(endpoint_url)
        hostname = parsed.hostname or parsed.netloc

        # Parse patterns like:
        # - s3.amazonaws.com -> us-east-1
        # - s3.us-west-2.amazonaws.com -> us-west-2
        # - bucket.s3.eu-central-1.amazonaws.com -> eu-central-1
        parts = hostname.split('.')
        if 's3' in parts:
            idx = parts.index('s3')
            if idx + 1 < len(parts) and parts[idx + 1] != 'amazonaws':
                return parts[idx + 1]

        return 'us-east-1'

    def generate_presigned_post(
        self,
        Bucket: str,
        Key: str,
        Fields: dict[str, Any] | None = ...,
        Conditions: list[Any] | dict[str, Any] | None = ...,
        ExpiresIn: int = 3600,
    ) -> dict[str, Any]:
        """Generate a presigned POST policy.

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_post.html

        Note:
            Not yet implemented. Use generate_presigned_url for GET/PUT operations.
        """
        raise NotImplementedError(
            "generate_presigned_post is not yet implemented. "
            "Use generate_presigned_url for GET/PUT operations."
        )

    def generate_presigned_url(
        self,
        ClientMethod: str,
        Params: Mapping[str, Any],
        ExpiresIn: int = 3600,
        HttpMethod: str = "",
    ) -> str:

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
        
        """Generate a presigned URL for an S3 operation.

        This method is boto3-compatible and maps to the underlying fast presigner.

        Args:
            ClientMethod: S3 operation name (e.g., 'get_object', 'put_object')
            Params: Operation parameters (must include 'Bucket' and 'Key')
            ExpiresIn: URL expiration time in seconds (max 604800 / 7 days)
            HttpMethod: Optional HTTP method override (GET, PUT, DELETE, etc.)

        Returns:
            Presigned URL string

        Raises:
            PresignError: If required parameters are missing or invalid

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html

        Example:
            >>> url = client.generate_presigned_url(
            ...     'get_object',
            ...     Params={'Bucket': 'mybucket', 'Key': 'myfile.txt'},
            ...     ExpiresIn=3600
            ... )
        """
        if Params is None:
            Params = {}

        # Extract bucket and key from params
        bucket = Params.get('Bucket')
        key = Params.get('Key')

        if not bucket:
            raise PresignError("Missing required parameter 'Bucket' in Params")
        if not key:
            raise PresignError("Missing required parameter 'Key' in Params")

        # Determine HTTP method from ClientMethod if not explicitly provided
        if HttpMethod is None:
            HttpMethod = self._client_method_to_http_method(ClientMethod)

        try:
            return self._presigner.generate_presigned_url(
                bucket=bucket,
                key=key,
                method=HttpMethod,
                expires=ExpiresIn
            )
        except ValueError as e:
            raise PresignError(str(e)) from e

    def _client_method_to_http_method(self, client_method: str) -> str:
        """Map boto3 client method name to HTTP method.

        Args:
            client_method: S3 client method name (e.g., 'get_object')

        Returns:
            HTTP method string (GET, PUT, DELETE, etc.)
        """
        method_map = {
            'get_object': 'GET',
            'put_object': 'PUT',
            'delete_object': 'DELETE',
            'head_object': 'HEAD',
            'list_objects': 'GET',
            'list_objects_v2': 'GET',
        }
        return method_map.get(client_method.lower(), 'GET')

    def head_bucket(self, Bucket: str, **kwargs):
        """Check if a bucket exists and is accessible.

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_bucket.html

        Note:
            Not yet implemented. This client focuses on presigned URL generation.
        """
        raise NotImplementedError(
            "head_bucket is not implemented. "
            "This client focuses on presigned URL generation."
        )

    def head_object(self, Bucket: str, Key: str, **kwargs):
        """Check if an object exists.

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html

        Note:
            Not yet implemented. This client focuses on presigned URL generation.
        """
        raise NotImplementedError(
            "head_object is not implemented. "
            "This client focuses on presigned URL generation."
        )

    def create_bucket(self, **kwargs):
        """Create a new S3 bucket.

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html

        Note:
            Not yet implemented. This client focuses on presigned URL generation.
        """
        raise NotImplementedError(
            "create_bucket is not implemented. "
            "This client focuses on presigned URL generation."
        )
