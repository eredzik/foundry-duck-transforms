import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from transforms.api.transform_df import Transform


@dataclass
class Source:
    external_system_rid: str
    secrets_config_location: str | None = None

    def get_secret(self, secret_name: str)->str:
        if self.secrets_config_location is None:
            raise ValueError("No secrets config location specified")
        with open(self.secrets_config_location, 'r') as f:
            secrets = json.load(f)
            system= secrets.get(self.external_system_rid)
            if not system:
                raise ValueError(f"No secrets for external system {self.external_system_rid}")
            key = system.get(secret_name)
            if not key:
                raise ValueError(f"No secret for key {key} in external system {self.external_system_rid}")
            return key
        raise NotImplementedError()

def external_systems(**kwargs: Source):
    def _external_systems(transform: "Transform"):
        transform.external_systems = kwargs
        return transform
    return _external_systems