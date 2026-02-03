from __future__ import annotations

__all__ = ["application"]

def __getattr__(name: str):
    if name == "application":
        from app.main import app as application
        
        return application
    raise AttributeError(name)
