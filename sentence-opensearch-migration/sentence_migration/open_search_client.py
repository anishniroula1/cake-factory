import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

from sentence_migration.config import (
    MIGRATION_WORKERS,
    OPENSEARCH_PORT,
    OPENSEARCH_SERVICE,
    Config,
)


class OpenSearchClient:
    def __init__(self, config: Config, iam_role: str):
        self.config = config
        self.iam_role = iam_role

    def create_client(self):
        """Create an AWS-signed client without pinging or creating storage."""

        self.config.validate_opensearch()
        session = self._session()
        credentials = session.get_credentials()
        if credentials is None:
            raise RuntimeError("The configured IAM role returned no AWS credentials")
        auth = AWSV4SignerAuth(
            credentials,
            self.config.aws_region,
            OPENSEARCH_SERVICE,
        )
        return OpenSearch(
            hosts=[
                {
                    "host": self.config.normalized_opensearch_host,
                    "port": OPENSEARCH_PORT,
                }
            ],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=True,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            pool_maxsize=MIGRATION_WORKERS * 4,
            timeout=60,
            max_retries=5,
            retry_on_timeout=True,
        )

    def _session(self) -> boto3.Session:
        """Assume the selected environment-approved OpenSearch role."""

        base_session = boto3.Session(region_name=self.config.aws_region)
        assume_args = {
            "RoleArn": self.iam_role,
            "RoleSessionName": "sentenceOpenSearchMigrationSession",
        }
        if self.config.aws_role_external_id:
            assume_args["ExternalId"] = self.config.aws_role_external_id
        response = base_session.client("sts").assume_role(**assume_args)
        credentials = response["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.config.aws_region,
        )
