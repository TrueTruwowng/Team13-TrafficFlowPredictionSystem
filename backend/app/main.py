from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.routers.datasets import router as datasets_router
from app.routers.maps import router as maps_router


app = FastAPI(title="TrafficFlow Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(datasets_router)
app.include_router(maps_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
