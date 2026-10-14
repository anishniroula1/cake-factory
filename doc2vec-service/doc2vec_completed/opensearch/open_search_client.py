import boto3
from opensearchpy import (
    AWSV4SignerAuth,
    OpenSearch,
    RequestsHttpConnection,
)

from doc2vec_completed.opensearch.config import Config


class OpenSearchClient:
    """Create an AWS-signed OpenSearch client for Doc2Vec indexing."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def create_client(self) -> OpenSearch:
        """Create a pooled OpenSearch client using the pod or assumed-role login.

        Input: Config with opensearch_host="search-sentences.us-east-1.es.amazonaws.com"
        Output: an AWS-signed OpenSearch client configured for timeout retries.
        """

        session = self._session()
        credentials = session.get_credentials()
        if credentials is None:
            raise RuntimeError("AWS credentials are required")
        auth = AWSV4SignerAuth(
            credentials,
            self.config.aws_region,
            self.config.opensearch_service,
        )
        return OpenSearch(
            hosts=[
                {
                    "host": self.config.opensearch_host,
                    "port": self.config.opensearch_port,
                }
            ],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=True,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
            timeout=60,
            max_retries=5,
            retry_on_timeout=True,
        )

    def _session(self) -> boto3.Session:
        """Resolve the AWS session used to sign OpenSearch requests.

        Input: aws_region="us-east-1" and an optional IAM role ARN.
        Output: the pod session or an assumed-role boto3 Session.
        """

        base_session = boto3.Session(region_name=self.config.aws_region)
        if not self.config.opensearch_iam_access_role:
            return base_session
        response = base_session.client("sts").assume_role(
            RoleArn=self.config.opensearch_iam_access_role,
            RoleSessionName="doc2vecSentenceIngestion",
        )
        credentials = response["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.config.aws_region,
        )
