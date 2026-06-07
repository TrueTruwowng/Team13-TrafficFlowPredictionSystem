from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.core.config import settings
from app.schemas import MapGeometryResponse
from app.services.map_repository import NghiaDoMapRepository


router = APIRouter(prefix="/maps", tags=["maps"])

nghia_do_repository = NghiaDoMapRepository(
    osm_xml_path=settings.nghia_do_osm_xml_path,
    net_xml_path=settings.nghia_do_net_xml_path,
)


@router.get("/nghia-do", response_model=MapGeometryResponse)
def get_nghia_do_map() -> MapGeometryResponse:
    try:
        return nghia_do_repository.load_geometry()
    except FileNotFoundError as error:
        raise HTTPException(status_code=500, detail=str(error)) from error
