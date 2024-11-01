import json
from dataclasses import dataclass

from ..transform_df import Transform


@dataclass
class ExternalSystemReq:
    external_system_rid: str
    secrets_config_location: str | None

    def get_secret(self, key: str)->str:
        if self.secrets_config_location is None:
            raise ValueError("No secrets config location specified")
        with open(self.secrets_config_location, 'r') as f:
            secrets = json.load(f)
            system= secrets.get(self.external_system_rid)
            if not system:
                raise ValueError(f"No secrets for external system {self.external_system_rid}")
            key = system.get(key)
            if not key:
                raise ValueError(f"No secret for key {key} in external system {self.external_system_rid}")
            return key
        raise NotImplementedError()

def external_systems(**kwargs: ExternalSystemReq):
    def _external_systems(transform: Transform):
        transform.external_systems = kwargs
        return transform
    return _external_systems