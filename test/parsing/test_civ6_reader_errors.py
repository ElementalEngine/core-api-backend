import pytest

from app.features.matches.parsers import civ6


def test_read_string_raises_on_truncated_buffer():
    buffer = bytes([0x04, 0x00, 0x00])
    with pytest.raises(ValueError, match="read_string"):
        civ6.read_string(buffer, {"pos": 0})


def test_read_string_raises_on_unknown_type_byte():
    buffer = bytes([0x04, 0x00, 0x00, 0x7F, 0, 0, 0, 0, 0, 0, 0, 0])
    with pytest.raises(ValueError, match="read_string"):
        civ6.read_string(buffer, {"pos": 0})


def test_read_utf_string_raises_on_unexpected_prefix():
    buffer = bytes([0x02, 0x00]) + bytes([0xFF] * 16)
    with pytest.raises(ValueError, match="read_utf_string"):
        civ6.read_utf_string(buffer, {"pos": 0})


def test_read_array_0b_raises_on_bad_element_marker():
    buffer = bytes(8) + bytes([0x01, 0, 0, 0]) + bytes([0xFF] * 32)
    with pytest.raises(ValueError, match="read_array_0b"):
        civ6.read_array_0b(buffer, {"pos": 0})
