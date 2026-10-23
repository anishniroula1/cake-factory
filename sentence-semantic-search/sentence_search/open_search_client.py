import boto3
from opensearchpy import (
    AWSV4SignerAuth,
    OpenSearch,
    RequestsHttpConnection,
)

from sentence_search.config import Config

OPENSEARCH_PORT = 443
OPENSEARCH_SERVICE = "es"


class OpenSearchClient:
    def __init__(self, config: Config):
        """Save the OpenSearch and AWS settings."""

        self.config = config

    def create_client(self):
        """Create an AWS-signed OpenSearch client."""

        session = self._session()
        credentials = session.get_credentials()
        if credentials is None:
            raise RuntimeError("AWS credentials are required")
        auth = AWSV4SignerAuth(
            credentials,
            self.config.aws_region,
            OPENSEARCH_SERVICE,
        )
        return OpenSearch(
            hosts=[
                {
                    "host": self.config.opensearch_host,
                    "port": OPENSEARCH_PORT,
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
        """Use the active AWS session or assume the configured role."""

        base_session = boto3.Session(region_name=self.config.aws_region)
        if not self.config.iam_access_role:
            return base_session
        response = base_session.client("sts").assume_role(
            RoleArn=self.config.iam_access_role,
            RoleSessionName="sentenceSemanticSearchSession",
        )
        credentials = response["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.config.aws_region,
        )
