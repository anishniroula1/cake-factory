import os
from pathlib import Path

import yaml


class VaultSecrets:
    def __init__(self, secret_file: str | None = None) -> None:
        """Find the Vault YAML file and read its values."""

        self.secret_file = secret_file or os.getenv("VAULT_YAML_PATH", "").strip()
        self.secrets = self._read_secrets()

    def get(self, name: str, default=None):
        """Return one Vault value, or the default when the name is missing."""

        return self.secrets.get(name, default)

    def _read_secrets(self) -> dict:
        """Read the YAML file and return its names and values as a dictionary."""

        if not self.secret_file:
            return {}

        secret_path = Path(self.secret_file).expanduser()
        if not secret_path.is_file():
            raise FileNotFoundError(f"Vault secret file was not found: {secret_path}")

        values = yaml.safe_load(secret_path.read_text(encoding="utf-8"))
        if values is None:
            return {}
        if not isinstance(values, dict):
            raise TypeError("Vault secret file must contain key and value pairs")
        return values
