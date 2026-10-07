import boto3
from opensearchpy import (
    AWSV4SignerAuth,
    OpenSearch,
    RequestsHttpConnection,
)

from sentence_similarity.config import Config


class OpenSearchClient:
    def __init__(self, config: Config) -> None:
        """Save the AWS and OpenSearch connection settings."""

        self.config = config

    def create_client(self) -> OpenSearch:
        """Create the AWS-signed client used by all sentence workers."""

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
            pool_maxsize=self.config.thread_worker_count,
            timeout=60,
            max_retries=5,
            retry_on_timeout=True,
        )

    def _session(self) -> boto3.Session:
        """Use pod credentials or assume the configured OpenSearch role."""

        base_session = boto3.Session(region_name=self.config.aws_region)
        if not self.config.opensearch_iam_access_role:
            return base_session
        response = base_session.client("sts").assume_role(
            RoleArn=self.config.opensearch_iam_access_role,
            RoleSessionName="sentenceSimilarity",
        )
        credentials = response["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.config.aws_region,
        )
