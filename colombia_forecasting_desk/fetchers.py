"""Compatibility facade for source-fetching internals.

The implementation lives in `colombia_forecasting_desk.source_fetching.core`.
This module remains the public import path used by the pipeline, tests, docs,
and external workflow snippets.
"""
from __future__ import annotations

import sys

from .source_fetching import core as _core

_MODULE_NAME = __name__
globals().update(_core.__dict__)
sys.modules[_MODULE_NAME] = _core
