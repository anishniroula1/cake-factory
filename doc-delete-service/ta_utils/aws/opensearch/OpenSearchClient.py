import boto3
from botocore.client import BaseClient
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import NoCredentialsError
from botocore.session import get_session
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection


class OpenSearchClient:
    """Create an AWS-signed OpenSearch client with refreshable credentials."""

    def __init__(
        self,
        opensearch_host: str,
        opensearch_iam_access_role: str,
        aws_region: str = "us-east-1",
        opensearch_port: int = 443,
        opensearch_service: str = "es",
    ) -> None:
        self.aws_region = aws_region
        self.opensearch_host = opensearch_host
        self.opensearch_port = opensearch_port
        self.opensearch_service = opensearch_service
        self.opensearch_iam_access_role = opensearch_iam_access_role

    def create_client(self) -> OpenSearch:
        """Create a pooled client that automatically refreshes an assumed-role token."""

        session = self._session()
        credentials = session.get_credentials()
        if credentials is None:
            raise NoCredentialsError()
        auth = AWSV4SignerAuth(credentials, self.aws_region, self.opensearch_service)
        return OpenSearch(
            hosts=[{"host": self.opensearch_host, "port": self.opensearch_port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=True,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            pool_maxsize=10,
            timeout=60,
            max_retries=5,
            retry_on_timeout=True,
        )

    def _session(self) -> boto3.Session:
        """Return pod credentials or a session that refreshes the assumed role."""

        base_session = boto3.Session(region_name=self.aws_region)
        if not self.opensearch_iam_access_role:
            return base_session

        sts_client = base_session.client("sts")

        def refresh_credentials() -> dict[str, str]:
            return self._assume_role_credentials(sts_client)

        credentials = RefreshableCredentials.create_from_metadata(
            metadata=refresh_credentials(),
            refresh_using=refresh_credentials,
            method="sts-assume-role",
        )
        botocore_session = get_session()
        botocore_session._credentials = credentials
        botocore_session.set_config_variable("region", self.aws_region)
        return boto3.Session(botocore_session=botocore_session)

    def _assume_role_credentials(self, sts_client: BaseClient) -> dict[str, str]:
        """Assume the configured role and return botocore refresh metadata."""

        response = sts_client.assume_role(
            RoleArn=self.opensearch_iam_access_role,
            RoleSessionName="documentDeletion",
        )
        credentials = response["Credentials"]
        return {
            "access_key": credentials["AccessKeyId"],
            "secret_key": credentials["SecretAccessKey"],
            "token": credentials["SessionToken"],
            "expiry_time": credentials["Expiration"].isoformat(),
        }
