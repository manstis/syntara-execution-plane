"""Public reads share one provider-independent client contract."""

from execution_plane.api.schemas import ClusterRead, ExecutionTargetRead, WorkItemRead
from execution_plane_client.resources import WorkItemRead as ClientWorkItemRead


def test_resource_reads_do_not_expose_invocation_or_management_credentials():
    assert WorkItemRead is ClientWorkItemRead
    for schema in (ClusterRead, ExecutionTargetRead, WorkItemRead):
        assert not {"payload", "credential", "api_key", "ca_certificate"} & schema.model_fields.keys()
