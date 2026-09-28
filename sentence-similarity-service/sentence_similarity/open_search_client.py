import boto3
from opensearchpy import (
    AWSV4SignerAuth,
    OpenSearch,
    RequestsHttpConnection,
)

from sentence_similarity.config import Config


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
            self.config.awsRegion,
            self.config.opensearchService,
        )
        return OpenSearch(
            hosts=[
                {
                    "host": self.config.opensearchHost,
                    "port": self.config.opensearchPort,
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

        baseSession = boto3.Session(region_name=self.config.awsRegion)
        if not self.config.iamAccessRole:
            return baseSession
        response = baseSession.client("sts").assume_role(
            RoleArn=self.config.iamAccessRole,
            RoleSessionName="sentenceSimilarity",
        )
        credentials = response["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.config.awsRegion,
        )
