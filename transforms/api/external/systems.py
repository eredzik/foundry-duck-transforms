from dataclasses import dataclass

from ..transform_df import Transform


@dataclass
class ExternalSystemReq:
    external_system_rid: str

def external_systems(*arg: ExternalSystemReq):
    def _external_systems(transform: Transform):
        transform.external_systems = list(arg)
        return transform
    return _external_systems