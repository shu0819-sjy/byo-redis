"""性能基准工具的纯函数测试。"""

from __future__ import annotations

import pytest

from tools.benchmark import build_command, percentile


def test_percentile_uses_nearest_rank() -> None:
    """百分位计算应采用最近秩并正确处理边界。"""
    values = [5.0, 1.0, 3.0, 2.0, 4.0]
    assert percentile(values, 0) == 1.0
    assert percentile(values, 0.5) == 3.0
    assert percentile(values, 1) == 5.0


def test_percentile_rejects_invalid_input() -> None:
    """空数据和非法比例必须给出明确错误。"""
    with pytest.raises(ValueError):
        percentile([], 0.5)
    with pytest.raises(ValueError):
        percentile([1.0], 1.1)


def test_build_command_generates_supported_commands() -> None:
    """基准命令生成器应覆盖 PING、SET、GET 并拒绝未知类型。"""
    assert build_command("ping", 0, 0) == [b"PING"]
    assert build_command("set", 2, 3) == [b"SET", b"benchmark:2:3", b"value"]
    assert build_command("get", 0, 0) == [b"GET", b"benchmark:shared"]
    with pytest.raises(ValueError):
        build_command("delete", 0, 0)
