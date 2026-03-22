"""Vercel entrypoint: discovers `app` per https://vercel.com/docs/frameworks/backend/flask"""

from web_demo import app

__all__ = ["app"]
