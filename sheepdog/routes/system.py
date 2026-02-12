from fastapi import APIRouter, Depends, FastAPI, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio.session import AsyncSession

# from ..db import get_db_session


router = APIRouter()


@router.get("/_version")
def get_version(request: Request) -> dict:
    """
    Returns the version of Sheepdog
    ---
    tags:
        - system
    responses:
        200:
        description: successful operation
    """
    #     dictver = {"version": dictionary_version(), "commit": dictionary_commit()}
    #     base = {"version": VERSION, "commit": COMMIT, "dictionary": dictver}
    return dict(version=request.app.version)


@router.get("/_status")
async def get_status(
    # db_session: AsyncSession = Depends(get_db_session),
) -> dict:
    # await db_session.execute(text("SELECT 1;"))
    return dict(status="OK")


def init_app(app: FastAPI) -> None:
    app.include_router(router, tags=["System"])
