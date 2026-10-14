import boto3
from botocore.exceptions import NoCredentialsError
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
        """Resolve the AWS session used to sign OpenSearch requests. Pod
        credentials are used unless an IAM role ARN is configured.

        Input: aws_region="us-east-1" and an optional IAM role ARN.
        Output: a boto3 Session using pod credentials or assumed-role credentials.
        """

        base_session = boto3.Session(region_name=self.config.aws_region)
        if not self.config.opensearch_iam_access_role:
            return base_session
        response = base_session.client("sts").assume_role(
            RoleArn=self.config.opensearch_iam_access_role, RoleSessionName="sentenceSimilarity"
        )
        credentials = response["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.config.aws_region,
        )
