from src.pgskewer import safe_json


def test_safe_json():
    # validate the examples in the safe_json docstring:
    assert safe_json('{"key": "value"}') == safe_json(b'{"key": "value"}') == {"key": "value"}
    assert safe_json("invalid json") is None
    assert safe_json(None) is None
