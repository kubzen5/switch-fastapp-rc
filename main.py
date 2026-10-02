"""Compatibility entry point: uvicorn main:app."""

from app.api.main import app

__all__ = ["app"]
