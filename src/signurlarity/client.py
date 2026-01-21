from typing import Any, Mapping


class Client:
    def __init__(
        self, endpoint_url: str, aws_access_key_id: str, aws_secret_access_key: str
    ):
        pass

    def generate_presigned_post(
        self,
        Bucket: str,
        Key: str,
        Fields: dict[str, Any] | None = ...,
        Conditions: list[Any] | dict[str, Any] | None = ...,
        ExpiresIn: int = 3600,
    ) -> dict[str, Any]:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_post.html
        raise NotImplementedError()

    def generate_presigned_url(
        self,
        ClientMethod: str,
        Params: Mapping[str, Any] = ...,
        ExpiresIn: int = 3600,
        HttpMethod: str = ...,
    ) -> str:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
        raise NotImplementedError()

    def head_bucket(self, Bucket: str, **kwargs):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_bucket.html
        raise NotImplementedError()

    def head_object(self, Bucket: str, Key: str, **kwargs):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html
        raise NotImplementedError()

    def create_bucket(self, **kwargs):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html
        raise NotImplementedError()
