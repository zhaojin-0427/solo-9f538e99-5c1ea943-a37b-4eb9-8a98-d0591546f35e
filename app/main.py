"""FastAPI application: daily PM2.5 hourly data publishing service."""
from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Path as PathParam, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import config, db
from .validation import CsvValidationError, parse_and_validate


@asynccontextmanager
async def lifespan(_app: FastAPI):
    config.ensure_dirs()
    db.init_db()
    yield


app = FastAPI(
    title="PM2.5 小时数据发布 API",
    description=(
        "按监测点和日期上传/补报每日 24 小时 PM2.5 数据。"
        "整批校验通过才发布；补报携带版本号做乐观并发控制，"
        "成功后整批替换，旧版本原始 CSV 永久留存。"
        "另提供监测站归属登记（源站编号 -> 物理站）与按物理站、"
        "日期范围读取已发布逐日数据的查询接口。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)

STATION_PATTERN = re.compile(r"^[A-Za-z0-9_\-.:]{1,64}$")
MAX_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MiB is far beyond any 24-row CSV


def _validate_station(station_id: str) -> str:
    station_id = station_id.strip()
    if not STATION_PATTERN.match(station_id):
        raise HTTPException(
            status_code=400,
            detail=(
                "station_id must be 1..64 chars of letters, digits or "
                "'_ - . :'"
            ),
        )
    return station_id


@app.exception_handler(CsvValidationError)
async def _csv_error_handler(_request, exc: CsvValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": "validation_failed",
            "message": "整批数据未通过校验，没有任何记录被发布",
            "reasons": exc.reasons,
        },
    )


@app.exception_handler(db.VersionConflictError)
async def _conflict_handler(_request, exc: db.VersionConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "error": "version_conflict",
            "message": "补报版本号与当前版本不一致，已有并发补报先生效",
            "expected_version": exc.expected,
            "current_version": exc.current,
        },
    )


@app.exception_handler(db.PublishError)
async def _publish_error_handler(_request, exc: db.PublishError) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "error": "publish_failed",
            "message": str(exc),
            "detail": "写入失败，上一版本仍完整可查",
        },
    )


@app.exception_handler(db.AttributionConflictError)
async def _attribution_conflict_handler(
    _request, exc: db.AttributionConflictError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "error": "attribution_conflict",
            "message": (
                "同一源站同一天不能归属不同物理站，登记未生效"
            ),
            "conflicts": exc.conflicts,
        },
    )


class AttributionRequest(BaseModel):
    physical_station_id: str = Field(
        ..., description="物理站编号（新号/合并后的站）"
    )
    source_station_id: str = Field(..., description="源站编号（旧号/新号）")
    start_date: date = Field(..., description="生效起始日（含）YYYY-MM-DD")
    end_date: date = Field(..., description="生效结束日（含）YYYY-MM-DD")


def _validate_range(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date 不能晚于 end_date（起止日均包含）",
        )


@app.post("/attributions", status_code=201)
def create_attribution(payload: AttributionRequest):
    """登记一个源站编号在日期区间内归属某个物理站。

    同一源站同一天只允许归属一个物理站；重复登记完全相同的区间幂等返回
    已有记录（``created=false``，HTTP 200）。并发登记由数据库写事务串行化，
    只有一个能生效，其余返回 409。
    """
    physical = _validate_station(payload.physical_station_id)
    source = _validate_station(payload.source_station_id)
    _validate_range(payload.start_date, payload.end_date)

    try:
        row = db.register_attribution(
            physical, source, payload.start_date, payload.end_date
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    created = row.pop("created")
    if not created:
        return JSONResponse(status_code=200, content=row)
    return row


@app.get("/attributions")
def query_attributions(
    physical_station_id: str | None = Query(None, description="按物理站过滤"),
    source_station_id: str | None = Query(None, description="按源站过滤"),
):
    """列出归属登记，可按物理站或源站编号过滤。"""
    if physical_station_id is not None:
        physical_station_id = _validate_station(physical_station_id)
    if source_station_id is not None:
        source_station_id = _validate_station(source_station_id)
    rows = db.list_attributions(physical_station_id, source_station_id)
    return {"count": len(rows), "attributions": rows}


@app.get("/physical-stations/{physical_station_id}/daily-series")
def read_physical_station_series(
    physical_station_id: str = PathParam(..., description="物理站编号"),
    start_date: date = Query(..., description="起始日（含）YYYY-MM-DD"),
    end_date: date = Query(..., description="结束日（含）YYYY-MM-DD"),
):
    """按物理站读取日期范围内每天的当前已发布数据。

    每天的源站编号由归属登记解析（旧号/新号可跨日期拼接）。查询在单个
    SQLite 读事务（同一快照）中完成：登记或补报与查询并发时，一次查询
    看到的归属和读数要么全属于提交前、要么全属于提交后。

    某天有多个归属源站同时发布了数据时，该天标记 ``source_conflict``
    并列出候选来源，不会任取其中一份。
    """
    physical_station_id = _validate_station(physical_station_id)
    _validate_range(start_date, end_date)
    return db.get_physical_station_series(
        physical_station_id, start_date, end_date
    )


@app.post("/stations/{station_id}/dates/{data_date}/uploads", status_code=201)
async def upload_batch(
    station_id: str = PathParam(..., description="监测点标识"),
    data_date: date = PathParam(..., description="数据日期 YYYY-MM-DD"),
    expected_version: int = Form(
        0,
        ge=0,
        description="当前版本号；首次上传传 0（默认），补报必须传当前版本号",
    ),
    file: UploadFile = File(..., description="含 hour,pm25 两列的 CSV"),
):
    """上传或补报某监测点某日的 24 小时数据（整批校验、整批替换）。"""
    station_id = _validate_station(station_id)

    raw = await file.read(MAX_UPLOAD_BYTES + 1)
    if not raw:
        raise HTTPException(status_code=400, detail="上传文件为空")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="上传文件超过 5 MiB 限制")

    # Raises CsvValidationError -> 422 with per-row / per-hour reasons.
    batch = parse_and_validate(raw)

    result = db.publish_batch(station_id, data_date, expected_version, batch)
    result["replaced"] = expected_version != 0
    return result


@app.get("/stations/{station_id}/dates/{data_date}/records")
def read_current_records(
    station_id: str = PathParam(...),
    data_date: date = PathParam(...),
):
    """查询当前版本的 24 条小时记录及文件标识。"""
    station_id = _validate_station(station_id)
    data = db.get_current_data(station_id, data_date)
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"监测点 {station_id} 在 {data_date.isoformat()} 尚无已发布数据",
        )
    return data


@app.get("/stations/{station_id}/dates/{data_date}/versions")
def read_versions(
    station_id: str = PathParam(...),
    data_date: date = PathParam(...),
):
    """列出该监测点该日期的全部已发布版本（含每版文件标识）。"""
    station_id = _validate_station(station_id)
    versions = db.list_versions(station_id, data_date)
    return {
        "station_id": station_id,
        "data_date": data_date.isoformat(),
        "count": len(versions),
        "versions": versions,
    }


@app.get("/stations/{station_id}/dates/{data_date}/versions/{version}/file")
def download_version_file(
    station_id: str = PathParam(...),
    data_date: date = PathParam(...),
    version: int = PathParam(..., ge=1),
):
    """下载指定版本留存的原始 CSV。"""
    station_id = _validate_station(station_id)
    row = db.get_version_file(station_id, data_date, version)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="指定版本不存在",
        )

    # Defense in depth: resolve and ensure the path stays inside FILES_DIR.
    target = (config.FILES_DIR / row["file_path"]).resolve()
    root = config.FILES_DIR.resolve()
    if root not in target.parents and target != root:
        raise HTTPException(status_code=400, detail="非法文件路径")
    if not target.is_file():
        raise HTTPException(status_code=410, detail="留存文件已丢失")

    return FileResponse(
        target,
        media_type="text/csv",
        filename=target.name,
        headers={"X-File-Id": row["file_id"], "X-Version": str(version)},
    )


@app.get("/health")
def health():
    return {"status": "ok"}
