import boto3
from botocore.client import BaseClient
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import NoCredentialsError
from botocore.session import get_session
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

from similarity_completed.opensearch import constants
from similarity_completed.opensearch.config import Config


class OpenSearchClient:
    """Create an AWS-signed OpenSearch client for the sentence-similarity service."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def create_client(self) -> OpenSearch:
        """Create an AWS-signed OpenSearch client with connection pooling. The
        shared client supports concurrent workers and timeout retries.

        Input: Config with opensearch_host="search-sentences.us-east-1.es.amazonaws.com"
        Output: a thread-safe, AWS-signed OpenSearch client with connection pooling.

        Raises:
            NoCredentialsError: If AWS credentials cannot be resolved.
        """

        session = self._session()
        credentials = session.get_credentials()
        if credentials is None:
            raise NoCredentialsError()
        auth = AWSV4SignerAuth(credentials, self.config.aws_region, constants.OPENSEARCH_SERVICE)
        return OpenSearch(
            hosts=[{"host": self.config.opensearch_host, "port": constants.OPENSEARCH_PORT}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=True,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            pool_maxsize=constants.THREAD_WORKER_COUNT,
            timeout=60,
            max_retries=5,
            retry_on_timeout=True,
        )

    def _session(self) -> boto3.Session:
        """Resolve the refreshable AWS session used to sign OpenSearch requests.

        Input: aws_region="us-east-1" and an optional IAM role ARN.
        Output: the pod session or an automatically refreshed assumed-role session.
        """

        base_session = boto3.Session(region_name=self.config.aws_region)
        if not self.config.opensearch_iam_access_role:
            return base_session

        sts_client = base_session.client("sts")

        def refresh_credentials() -> dict[str, str]:
            return self._assume_role_credentials(sts_client)

        refreshable_credentials = RefreshableCredentials.create_from_metadata(
            metadata=refresh_credentials(),
            refresh_using=refresh_credentials,
            method="sts-assume-role",
        )
        botocore_session = get_session()
        botocore_session._credentials = refreshable_credentials
        botocore_session.set_config_variable("region", self.config.aws_region)
        return boto3.Session(botocore_session=botocore_session)

    def _assume_role_credentials(self, sts_client: BaseClient) -> dict[str, str]:
        """Assume the OpenSearch role and return botocore refresh metadata.

        Botocore calls this method again before the temporary credentials expire,
        so the shared OpenSearch client can continue signing requests.
        """

        response = sts_client.assume_role(
            RoleArn=self.config.opensearch_iam_access_role, RoleSessionName="sentenceSimilarity"
        )
        credentials = response["Credentials"]
        return {
            "access_key": credentials["AccessKeyId"],
            "secret_key": credentials["SecretAccessKey"],
            "token": credentials["SessionToken"],
            "expiry_time": credentials["Expiration"].isoformat(),
        }
