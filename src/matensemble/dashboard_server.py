import json
import pathlib

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


def create_app(status_file: str) -> FastAPI:

    app = FastAPI()
    app.add_middleware(CORSMiddleware, allow_origins=["*"])

    status_path = pathlib.Path(status_file)

    @app.get("/api/status")
    def get_status():
        try:
            return json.loads(status_path.read_text())
        except FileNotFoundError:
            return {
                "pending": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
                "freeCores": 0,
                "freeGpus": 0,
            }

    BASE_DIR = Path(__file__).resolve().parent
    DIST_DIR = BASE_DIR / "dash"
    app.mount("/", StaticFiles(directory=DIST_DIR, html=True), name="static")

    return app
