"""Public command and resource contracts for the standalone API."""

from execution_plane.api.main import create_app


def test_execution_target_response_uses_discriminated_placement() -> None:
    schema = create_app().openapi()
    target_schema = schema["components"]["schemas"]["ExecutionTargetRead"]
    assert "namespace" not in target_schema["properties"]
    assert target_schema["properties"]["placement"]["discriminator"]["propertyName"] == "type"


def test_command_contract_includes_routing_scope_and_execution_policy() -> None:
    schema = create_app().openapi()
    body = schema["paths"]["/v1/commands"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    assert {"message_id", "command_id", "scope", "command"} <= body["properties"].keys()
    assert {"target_url", "callback_url", "execution_policy"} <= body["properties"]["command"]["properties"].keys()
