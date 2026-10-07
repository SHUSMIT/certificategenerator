"""
Bulk Certificate Generator  -  single-file backend (FastAPI + SQLAlchemy + ReportLab)
=====================================================================================

README
------
SETUP
    python -m venv .venv && source .venv/bin/activate        # Windows: .venv\\Scripts\\activate
    pip install fastapi "sqlalchemy>=2" reportlab uvicorn httpx pytest

RUN
    python app.py serve                  # http://127.0.0.1:8000   (interactive docs: /docs)
    CERT_DB_URL=sqlite:///./certs.db CERT_STORAGE_DIR=./generated python app.py serve

TEST
    python app.py test                   # same as: pytest -v app.py

SUBMIT A REQUEST  (1 request = up to 200 recipients; 5/10/15/50/100/200 are all just list sizes)
    curl -X POST http://127.0.0.1:8000/jobs -H "Content-Type: application/json" -d '{
      "event_name": "Advanced Strategic Innovation Workshop 2026",
      "certificate_type": "Achievement",
      "issuer_name": "Jonathan Patterson",
      "issuer_title": "Program Director",
      "issue_date": "2026-03-03",
      "recipients": [
        {"name": "Harumi Kobayashi", "email": "harumi@example.com"},
        {"name": "Aarav Sharma"},
        {"name": "   "}
      ]
    }'
    -> 202 {"job_id": "...", "status": "pending", "total": 3, "status_url": "/jobs/<id>", ...}

CHECK PROGRESS
    curl http://127.0.0.1:8000/jobs/<job_id>
    -> status: pending | processing | completed | completed_with_errors | failed
       progress_percent, succeeded, failed, and one entry per recipient with its own
       status / error / download_url.

RETRIEVE CERTIFICATES
    one PDF : GET /jobs/<job_id>/certificates/<certificate_id>
    all (ZIP): GET /jobs/<job_id>/download            (only successfully generated ones)

DESIGN DECISIONS
  * Processing = BACKGROUND (FastAPI BackgroundTasks). POST returns 202 immediately with a job id,
    the client polls GET /jobs/{id}. Reason: the API is meant for 200+ recipients per request;
    holding an HTTP connection open for that long invites timeouts. Rendering one PDF takes only
    a few ms, so an in-process task is enough; no Celery/Redis to set up for an assignment.
    Trade-off: if the process dies mid-job, work stops. Mitigation: every certificate is a DB row
    with its own status, and on startup unfinished jobs are resumed (only 'pending' rows re-run).
    For real scale you'd swap BackgroundTasks for a queue (Celery/RQ) - process_job() is the unit.
  * Two validation levels. Request-level problems (no recipients, >200, missing event_name, bad
    date) -> 422, nothing is created. Per-recipient problems (blank name, bad email, characters
    the font cannot draw) -> that recipient is stored as 'failed' with a reason and the rest of the
    job proceeds. A crash while rendering one certificate is caught the same way.
  * Relational DB: SQLAlchemy 2.0, SQLite by default (set CERT_DB_URL for Postgres etc.).
    Tables: jobs (1) -> certificates (many). Counters on the job are updated after every
    certificate, so progress is live.
  * Template: one hard-coded design drawn with ReportLab vector graphics (A4 portrait, blue
    swooshes, gold seal, dashed side accents) modelled on the supplied PPTX - no asset files
    needed, so it stays a truly single-file project. Name auto-shrinks to fit; long text wraps.
  * Files are stored as <storage_dir>/<job_id>/<certificate_id>.pdf; file names never contain
    user input, so there is no path-traversal risk.
"""
from __future__ import annotations

import io
import csv
import math
import os
import re
import sys
import threading
import uuid
import zipfile
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field, ValidationError, field_validator
from reportlab.lib.colors import Color, HexColor, white
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import simpleSplit
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas as rl_canvas
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

MAX_RECIPIENTS = 200
DEFAULT_DB_URL = os.getenv("CERT_DB_URL", "sqlite:///./certificates.db")
DEFAULT_STORAGE = os.getenv("CERT_STORAGE_DIR", "./generated_certificates")


# ════════════════════════════════════════════════════════════════════════════
# 1. DATABASE MODELS
# ════════════════════════════════════════════════════════════════════════════
class Base(DeclarativeBase):
    pass


def _now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def _uid() -> str:
    """Create a compact unique identifier for database rows."""
    return uuid.uuid4().hex


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uid)
    status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    event_name: Mapped[str] = mapped_column(String(200))
    certificate_type: Mapped[str] = mapped_column(String(40))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_name: Mapped[str] = mapped_column(String(100))
    issuer_title: Mapped[str] = mapped_column(String(100))
    issue_date: Mapped[date]
    total: Mapped[int] = mapped_column(Integer)
    succeeded: Mapped[int] = mapped_column(Integer, default=0)
    failed: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    certificates: Mapped[list["Certificate"]] = relationship(
        back_populates="job", order_by="Certificate.position", cascade="all, delete-orphan"
    )


class Certificate(Base):
    __tablename__ = "certificates"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), index=True)
    position: Mapped[int] = mapped_column(Integer)  # index in the submitted list (0-based)
    recipient_name: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    recipient_email: Mapped[Optional[str]] = mapped_column(String(254), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="pending")  # pending|success|failed
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    file_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    job: Mapped[Job] = relationship(back_populates="certificates")


# ════════════════════════════════════════════════════════════════════════════
# 2. REQUEST SCHEMAS / VALIDATION
# ════════════════════════════════════════════════════════════════════════════
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class Recipient(BaseModel):
    """Validated per recipient (so one bad row never rejects the whole request)."""
    name: str = Field(min_length=1, max_length=100)
    email: Optional[str] = Field(default=None, max_length=254)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = " ".join(v.split())
        if not v:
            raise ValueError("name must not be blank")
        if any(ord(c) < 32 for c in v):
            raise ValueError("name contains control characters")
        return v

    @field_validator("email")
    @classmethod
    def _email(cls, v: Optional[str]) -> Optional[str]:
        if v is None or not v.strip():
            return None
        v = v.strip()
        if not _EMAIL_RE.match(v):
            raise ValueError("invalid email address")
        return v


class JobRequest(BaseModel):
    event_name: str = Field(min_length=1, max_length=200)
    certificate_type: str = Field(default="Achievement", min_length=1, max_length=30)
    description: Optional[str] = Field(default=None, max_length=400)
    issuer_name: str = Field(default="Authorized Signatory", min_length=1, max_length=100)
    issuer_title: str = Field(default="Program Director", min_length=1, max_length=100)
    issue_date: date = Field(default_factory=date.today)
    # Items stay raw dicts here on purpose; each is validated individually in create_job().
    recipients: list[Any] = Field(min_length=1, max_length=MAX_RECIPIENTS)

    @field_validator("event_name", "issuer_name", "issuer_title", "certificate_type")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        v = " ".join(v.split())
        if not v:
            raise ValueError("must not be blank")
        return v


# ════════════════════════════════════════════════════════════════════════════
# 3. CERTIFICATE TEMPLATE  (the single predefined design, drawn with ReportLab)
# ════════════════════════════════════════════════════════════════════════════
NAVY = HexColor("#0B1F4B")
ROYAL = HexColor("#3355BB")
BRIGHT = HexColor("#3F6BFF")
DEEP = HexColor("#2A4FB8")
TEXT = HexColor("#101828")

_UNICODE_FONT_CANDIDATES = [
    ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
    ("/Library/Fonts/Arial Unicode.ttf", "/Library/Fonts/Arial Unicode.ttf"),
    ("C:/Windows/Fonts/arial.ttf", "C:/Windows/Fonts/arialbd.ttf"),
]
_font_lock = threading.Lock()
_unicode_fonts: Optional[tuple[str, str]] = None
_unicode_checked = False


def _get_unicode_fonts() -> Optional[tuple[str, str]]:
    """Registers a system TTF once; used only when a name has non-Latin characters."""
    global _unicode_fonts, _unicode_checked
    with _font_lock:
        if _unicode_checked:
            return _unicode_fonts
        _unicode_checked = True
        for regular, bold in _UNICODE_FONT_CANDIDATES:
            if Path(regular).exists() and Path(bold).exists():
                pdfmetrics.registerFont(TTFont("CertUni", regular))
                pdfmetrics.registerFont(TTFont("CertUni-Bold", bold))
                _unicode_fonts = ("CertUni", "CertUni-Bold")
                break
        return _unicode_fonts


def _is_latin(text: str) -> bool:
    """Report whether text can be encoded by the built-in PDF font."""
    try:
        text.encode("cp1252")
        return True
    except UnicodeEncodeError:
        return False


def pick_fonts(*texts: str) -> tuple[str, str]:
    """(regular, bold) font names able to draw every given string, else raise ValueError."""
    if all(_is_latin(t) for t in texts):
        return "Helvetica", "Helvetica-Bold"
    fonts = _get_unicode_fonts()
    if fonts is None:
        raise ValueError("name contains characters this server has no font for")
    return fonts


def ordinal_date(d: date) -> str:
    """Format a date in the certificate's human-readable style."""
    suffix = "th" if 10 <= d.day % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(d.day % 10, "th")
    return f"{d.day}{suffix} day of {d.strftime('%B')}, {d.year}"


_LEAD_IN = {
    "achievement": "for successfully fulfilling the requirements of the",
    "participation": "for active participation in the",
    "completion": "for successfully completing the",
    "excellence": "for outstanding performance in the",
}


def _fit_font_size(text: str, font: str, max_size: float, max_width: float, min_size: float = 12) -> float:
    """Find the largest font size that fits within a width."""
    size = max_size
    while size > min_size and pdfmetrics.stringWidth(text, font, size) > max_width:
        size -= 0.5
    return size


def _draw_background(c: rl_canvas.Canvas, W: float, H: float) -> None:
    """Draw the fixed decorative background for the certificate template."""
    # soft pale waves (top-right and bottom)
    c.setFillColor(Color(0.93, 0.95, 0.99))
    p = c.beginPath()
    p.moveTo(W * 0.35, H)
    p.curveTo(W * 0.5, H - 90, W * 0.8, H - 120, W, H - 80)
    p.lineTo(W, H)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(Color(0.92, 0.94, 0.98))
    p = c.beginPath()
    p.moveTo(0, 0)
    p.lineTo(0, 95)
    p.curveTo(W * 0.3, 40, W * 0.55, 135, W, 70)
    p.lineTo(W, 0)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(Color(0.86, 0.89, 0.97))
    p = c.beginPath()
    p.moveTo(0, 0)
    p.lineTo(0, 50)
    p.curveTo(W * 0.35, 85, W * 0.6, 10, W, 40)
    p.lineTo(W, 0)
    p.close()
    c.drawPath(p, fill=1, stroke=0)

    # top-left swooshes: pale grey, bright blue, deep blue
    c.setFillColor(HexColor("#EEF1F8"))
    p = c.beginPath()
    p.moveTo(0, H - 215)
    p.curveTo(110, H - 205, 205, H - 130, 218, H)
    p.lineTo(0, H)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(BRIGHT)
    p = c.beginPath()
    p.moveTo(0, H - 135)
    p.curveTo(90, H - 130, 175, H - 85, 190, H)
    p.lineTo(0, H)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(HexColor("#F3F4F7"))
    p = c.beginPath()
    p.moveTo(0, H - 118)
    p.curveTo(80, H - 118, 150, H - 70, 168, H)
    p.lineTo(0, H)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(DEEP)
    p = c.beginPath()
    p.moveTo(0, H - 100)
    p.curveTo(70, H - 98, 125, H - 55, 140, H)
    p.lineTo(0, H)
    p.close()
    c.drawPath(p, fill=1, stroke=0)

    # soft sweeping layers descending from the corner (pale, then medium blue)
    c.setFillColor(Color(0.90, 0.92, 0.98))
    p = c.beginPath()
    p.moveTo(0, H - 205)
    p.curveTo(80, H - 220, 135, H - 262, 150, H - 335)
    p.curveTo(95, H - 318, 40, H - 316, 0, H - 326)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(HexColor("#2F5CE6"))
    p = c.beginPath()
    p.moveTo(0, H - 205)
    p.curveTo(75, H - 218, 128, H - 258, 148, H - 328)
    p.curveTo(146, H - 285, 118, H - 240, 70, H - 226)
    p.curveTo(40, H - 218, 15, H - 216, 0, H - 217)
    p.close()
    c.drawPath(p, fill=1, stroke=0)

    # bottom-right swooshes
    c.setFillColor(BRIGHT)
    p = c.beginPath()
    p.moveTo(W, 150)
    p.curveTo(W - 15, 100, W - 70, 40, W - 150, 0)
    p.lineTo(W, 0)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(DEEP)
    p = c.beginPath()
    p.moveTo(W, 85)
    p.curveTo(W - 30, 55, W - 70, 20, W - 100, 0)
    p.lineTo(W, 0)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(HexColor("#EEF1F8"))
    p = c.beginPath()
    p.moveTo(W, 205)
    p.curveTo(W - 25, 130, W - 85, 60, W - 175, 0)
    p.lineTo(W - 150, 0)
    p.curveTo(W - 70, 45, W - 20, 110, W, 150)
    p.close()
    c.drawPath(p, fill=1, stroke=0)

    # dashed accent ticks down the left and right edges (fading out)
    c.setStrokeColor(NAVY)
    c.setLineCap(1)
    for i in range(21):
        y = H - 395 - i * 18.5
        fade = max(0.0, 1 - i / 22)
        c.setLineWidth(1.4)
        c.setStrokeAlpha(0.25 + 0.65 * fade)
        n = max(1, int(14 * fade) + 1)
        for k in range(n):
            x = 3 + k * 4.2
            c.line(x, y, x, y + 7)
    for i in range(20):
        y = H - 470 - i * 19
        fade = max(0.0, 1 - i / 24)
        c.setStrokeAlpha(0.2 + 0.5 * fade)
        c.setLineWidth(1.1)
        n = int(2 + 9 * (i / 20))
        for k in range(n):
            x = W - 4 - k * 4.4
            c.line(x, y, x, y + 5)
    c.setStrokeAlpha(1)


def _draw_seal(c: rl_canvas.Canvas, cx: float, cy: float, r: float) -> None:
    """Draw the gold seal used by the predefined certificate template."""
    scallops = 26
    path = c.beginPath()
    steps = scallops * 12
    for i in range(steps + 1):
        a = 2 * math.pi * i / steps
        rr = r * (0.93 + 0.07 * math.cos(scallops * a))
        x, y = cx + rr * math.cos(a), cy + rr * math.sin(a)
        path.moveTo(x, y) if i == 0 else path.lineTo(x, y)
    path.close()
    c.saveState()
    c.setFillColor(HexColor("#A87A1C"))
    c.drawPath(path, fill=1, stroke=0)  # darker rim / shadow base
    c.clipPath(path, stroke=0, fill=0)
    c.radialGradient(cx - r * 0.1, cy + r * 0.1, r * 1.05,
                     [HexColor("#FBEAA6"), HexColor("#E3B94B"), HexColor("#B98A22")], [0, 0.55, 1])
    c.restoreState()
    c.setLineWidth(1.2)
    c.setStrokeColor(HexColor("#F7DE86"))
    c.circle(cx, cy, r * 0.74, stroke=1, fill=0)
    c.saveState()
    inner = c.beginPath()
    inner.circle(cx, cy, r * 0.70)
    c.clipPath(inner, stroke=0, fill=0)
    c.radialGradient(cx, cy, r * 0.7, [HexColor("#F4D878"), HexColor("#C9982E"), HexColor("#EBCB6A")], [0, 0.6, 1])
    c.restoreState()
    c.setStrokeColor(HexColor("#9C7218"))
    c.setLineWidth(0.8)
    c.circle(cx, cy, r * 0.70, stroke=1, fill=0)


def _draw_centered_wrapped(c, text, font, size, y, max_width, leading, W) -> float:
    """Draws wrapped, centred text; returns the y position below the block."""
    c.setFont(font, size)
    for line in simpleSplit(text, font, size, max_width):
        c.drawCentredString(W / 2, y, line)
        y -= leading
    return y


def render_certificate(path: Path, *, name: str, event_name: str, certificate_type: str,
                       description: Optional[str], issue_date: date,
                       issuer_name: str, issuer_title: str) -> None:
    """Renders ONE certificate PDF to `path`. Raises ValueError for un-renderable input."""
    reg, bold = pick_fonts(name, event_name, certificate_type, description or "", issuer_name, issuer_title)
    W, H = A4
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    c = rl_canvas.Canvas(str(tmp), pagesize=A4)
    c.setTitle(f"Certificate - {name}")
    c.setAuthor(issuer_name)

    _draw_background(c, W, H)
    _draw_seal(c, W / 2 + 4, H - 105, 50)

    c.setFillColor(NAVY)
    c.setFont("Helvetica-Bold", 44)
    c.drawCentredString(W / 2, H - 265, "CERTIFICATE")
    sub = f"OF {certificate_type.upper()}"
    sub_font = reg
    size = _fit_font_size(sub, sub_font, 17, 360, 10)
    t = c.beginText()
    t.setFont(sub_font, size)
    t.setCharSpace(2.4)
    width = pdfmetrics.stringWidth(sub, sub_font, size) + 2.4 * len(sub)
    t.setTextOrigin(W / 2 - width / 2, H - 290)
    t.textOut(sub)
    t.setCharSpace(0)  # spacing is part of the text state; reset so later text is unaffected
    c.drawText(t)

    c.setFillColor(TEXT)
    c.setFont(reg, 12)
    c.drawCentredString(W / 2, H - 352, "This certificate is proudly presented to")

    # recipient name - the focal point
    name_size = _fit_font_size(name, reg, 38, W - 190)
    c.setFillColor(ROYAL)
    c.setFont(reg, name_size)
    c.drawCentredString(W / 2, H - 405, name)
    nw = min(pdfmetrics.stringWidth(name, reg, name_size) + 40, W - 150)
    c.setStrokeColor(HexColor("#B8C4E8"))
    c.setLineWidth(0.8)
    c.line(W / 2 - nw / 2, H - 418, W / 2 + nw / 2, H - 418)

    c.setFillColor(TEXT)
    lead = _LEAD_IN.get(certificate_type.lower(), "for successfully completing the")
    c.setFont(reg, 12)
    c.drawCentredString(W / 2, H - 452, lead)
    y = _draw_centered_wrapped(c, event_name, bold, 15, H - 474, W - 190, 19, W)
    desc = description or "In recognition of dedication, consistent effort and a valuable contribution."
    y = _draw_centered_wrapped(c, desc, reg, 11.5, y - 6, W - 200, 16, W)

    c.setFont(bold, 12.5)
    c.drawCentredString(W / 2, min(y - 28, H - 600), f"Awarded on the {ordinal_date(issue_date)}")

    # signature block
    sig_y = 150
    c.setStrokeColor(NAVY)
    c.setLineWidth(0.8)
    c.line(W / 2 - 80, sig_y + 20, W / 2 + 80, sig_y + 20)
    c.setFont(bold, 13)
    c.drawCentredString(W / 2, sig_y, issuer_name)
    c.setFont(reg, 11.5)
    c.drawCentredString(W / 2, sig_y - 16, issuer_title)

    c.showPage()
    c.save()
    os.replace(tmp, path)  # atomic: a half-written file is never visible as a certificate


# ════════════════════════════════════════════════════════════════════════════
# 4. JOB PROCESSING
# ════════════════════════════════════════════════════════════════════════════
def _final_status(job: Job) -> str:
    """Calculate the final job status from successful and failed counts."""
    if job.succeeded == job.total:
        return "completed"
    if job.succeeded == 0:
        return "failed"
    return "completed_with_errors"


def process_job(session_factory: sessionmaker, storage_dir: Path, job_id: str) -> None:
    """Renders every still-pending certificate of a job. Safe to call again (resumable)."""
    with session_factory() as s:
        job = s.get(Job, job_id)
        if job is None:
            return
        job.status = "processing"
        s.commit()
        pending = s.scalars(
            select(Certificate).where(Certificate.job_id == job_id, Certificate.status == "pending")
            .order_by(Certificate.position)
        ).all()
        for cert in pending:
            try:
                out = storage_dir / job_id / f"{cert.id}.pdf"
                render_certificate(
                    out, name=cert.recipient_name, event_name=job.event_name,
                    certificate_type=job.certificate_type, description=job.description,
                    issue_date=job.issue_date, issuer_name=job.issuer_name, issuer_title=job.issuer_title,
                )
                cert.status, cert.file_path, cert.error = "success", str(out), None
                job.succeeded += 1
            except Exception as exc:  # one bad certificate must never kill the job
                cert.status, cert.error = "failed", f"{type(exc).__name__}: {exc}"[:500]
                job.failed += 1
            s.commit()  # commit per certificate -> live progress for pollers
        job.status = _final_status(job)
        job.completed_at = _now()
        s.commit()


# ════════════════════════════════════════════════════════════════════════════
# 5. API
# ════════════════════════════════════════════════════════════════════════════
def _cert_dict(job_id: str, c: Certificate) -> dict:
    """Serialize one certificate row for the status API."""
    return {
        "certificate_id": c.id,
        "position": c.position,
        "recipient_name": c.recipient_name,
        "recipient_email": c.recipient_email,
        "status": c.status,
        "error": c.error,
        "download_url": f"/jobs/{job_id}/certificates/{c.id}" if c.status == "success" else None,
    }


def _job_dict(job: Job, include_items: bool = True) -> dict:
    """Serialize a job and its live progress counters."""
    done = job.succeeded + job.failed
    data = {
        "job_id": job.id,
        "status": job.status,
        "event_name": job.event_name,
        "certificate_type": job.certificate_type,
        "total": job.total,
        "succeeded": job.succeeded,
        "failed": job.failed,
        "pending": job.total - done,
        "progress_percent": round(100 * done / job.total, 1) if job.total else 100.0,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "status_url": f"/jobs/{job.id}",
        "download_all_url": f"/jobs/{job.id}/download",
    }
    if include_items:
        data["certificates"] = [_cert_dict(job.id, c) for c in job.certificates]
    return data


def create_app(db_url: str = DEFAULT_DB_URL, storage_dir: str | Path = DEFAULT_STORAGE) -> FastAPI:
    """Build a FastAPI application backed by the requested database and storage."""
    connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
    engine = create_engine(db_url, connect_args=connect_args)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(engine, expire_on_commit=False)
    storage = Path(storage_dir)
    storage.mkdir(parents=True, exist_ok=True)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        """Resume interrupted jobs when the application starts."""
        # Resume jobs interrupted by a restart (only their 'pending' rows are re-rendered).
        with session_factory() as s:
            ids = s.scalars(select(Job.id).where(Job.status.in_(["pending", "processing"]))).all()
        for jid in ids:
            threading.Thread(target=process_job, args=(session_factory, storage, jid), daemon=True).start()
        yield

    app = FastAPI(title="Bulk Certificate Generator", version="1.0", lifespan=lifespan)
    app.state.session_factory = session_factory
    app.state.storage = storage

    def get_db(request: Request):
        """Yield a database session for one API request."""
        with request.app.state.session_factory() as s:
            yield s

    def _get_job_or_404(db: Session, job_id: str) -> Job:
        """Load a job or raise the API's standard 404 response."""
        job = db.get(Job, job_id)
        if job is None:
            raise HTTPException(404, "Job not found")
        return job

    @app.post("/jobs", status_code=202)
    def create_job(req: JobRequest, background: BackgroundTasks, db: Session = Depends(get_db)):
        """Create a bulk generation job. Valid recipients are queued; invalid ones are recorded as failed."""
        return _create_job_from_recipients(req, background, db)

    def _create_job_from_recipients(req: JobRequest, background: BackgroundTasks, db: Session) -> dict:
        """Create and queue a job from already parsed recipient rows."""
        job = Job(
            event_name=req.event_name, certificate_type=req.certificate_type, description=req.description,
            issuer_name=req.issuer_name, issuer_title=req.issuer_title, issue_date=req.issue_date,
            total=len(req.recipients),
        )
        db.add(job)
        db.flush()
        for pos, raw in enumerate(req.recipients):
            cert = Certificate(job_id=job.id, position=pos)
            try:
                if not isinstance(raw, dict):
                    raise ValueError("recipient must be an object like {\"name\": \"...\"}")
                r = Recipient.model_validate(raw)
                cert.recipient_name, cert.recipient_email = r.name, r.email
            except (ValidationError, ValueError) as exc:
                if isinstance(raw, dict):
                    nm = raw.get("name")
                    cert.recipient_name = str(nm)[:300] if nm is not None else None
                if isinstance(exc, ValidationError):
                    msg = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
                else:
                    msg = str(exc)
                cert.status, cert.error = "failed", f"Invalid recipient - {msg}"[:500]
                job.failed += 1
            db.add(cert)
        if job.failed == job.total:  # nothing valid to render
            job.status, job.completed_at = "failed", _now()
        db.commit()
        if job.failed < job.total:
            background.add_task(process_job, session_factory, storage, job.id)
        body = _job_dict(job, include_items=False)
        body["rejected_at_validation"] = job.failed
        return body

    @app.post("/jobs/csv", status_code=202)
    async def create_job_from_csv(
        background: BackgroundTasks,
        file: UploadFile = File(..., description="CSV with a name column and optional email column"),
        event_name: str = Form(...),
        certificate_type: str = Form("Achievement"),
        description: Optional[str] = Form(None),
        issuer_name: str = Form("Authorized Signatory"),
        issuer_title: str = Form("Program Director"),
        issue_date: date = Form(default_factory=date.today),
        db: Session = Depends(get_db),
    ):
        """Parse a name-only or name/email CSV and create one bulk job."""
        if not file.filename or not file.filename.lower().endswith(".csv"):
            raise HTTPException(422, "file must have a .csv extension")
        try:
            raw = await file.read()
            text = raw.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text))
            headers = {h.strip().lower() for h in (reader.fieldnames or []) if h}
            name_header = next((h for h in ("name", "full_name", "recipient_name") if h in headers), None)
            email_header = next((h for h in ("email", "email_id", "mail", "recipient_email") if h in headers), None)
            if name_header is None:
                raise HTTPException(422, "CSV must contain a name column")
            recipients = []
            for row in reader:
                normalized = {str(k).strip().lower(): (v or "").strip() for k, v in row.items() if k}
                recipients.append({
                    "name": normalized.get(name_header, ""),
                    "email": normalized.get(email_header) if email_header else None,
                })
            if not recipients:
                raise HTTPException(422, "CSV must contain at least one data row")
        except UnicodeDecodeError as exc:
            raise HTTPException(422, "CSV must be UTF-8 encoded") from exc
        request_data = JobRequest(
            event_name=event_name,
            certificate_type=certificate_type,
            description=description,
            issuer_name=issuer_name,
            issuer_title=issuer_title,
            issue_date=issue_date,
            recipients=recipients,
        )
        return _create_job_from_recipients(request_data, background, db)

    @app.get("/jobs/{job_id}")
    def job_status(job_id: str, db: Session = Depends(get_db)):
        """Return live progress and per-recipient results for a job."""
        return _job_dict(_get_job_or_404(db, job_id))

    @app.get("/jobs/{job_id}/certificates/{certificate_id}")
    def get_certificate(job_id: str, certificate_id: str, db: Session = Depends(get_db)):
        """Return one generated certificate PDF."""
        cert = db.get(Certificate, certificate_id)
        if cert is None or cert.job_id != job_id:
            raise HTTPException(404, "Certificate not found")
        if cert.status != "success" or not cert.file_path or not Path(cert.file_path).exists():
            raise HTTPException(409, f"Certificate not available (status: {cert.status})")
        safe = re.sub(r"[^\w\-]+", "_", cert.recipient_name or "certificate", flags=re.UNICODE).strip("_") or "certificate"
        return FileResponse(cert.file_path, media_type="application/pdf", filename=f"{safe}.pdf")

    @app.get("/jobs/{job_id}/download")
    def download_all(job_id: str, db: Session = Depends(get_db)):
        """Return all successful certificates for a job as a ZIP archive."""
        job = _get_job_or_404(db, job_id)
        ok = [c for c in job.certificates if c.status == "success" and c.file_path and Path(c.file_path).exists()]
        if not ok:
            raise HTTPException(409, f"No certificates available yet (job status: {job.status})")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as z:  # PDFs are already compressed
            for c in ok:
                safe = re.sub(r"[^\w\-]+", "_", c.recipient_name or "certificate", flags=re.UNICODE).strip("_") or "certificate"
                z.write(c.file_path, f"{c.position + 1:03d}_{safe}.pdf")
        return Response(buf.getvalue(), media_type="application/zip",
                        headers={"Content-Disposition": f'attachment; filename="certificates_{job_id}.zip"'})

    return app


# ════════════════════════════════════════════════════════════════════════════
# 6. TESTS      (run with:  python app.py test     or     pytest -v app.py)
# ════════════════════════════════════════════════════════════════════════════
try:
    import pytest
    from fastapi.testclient import TestClient
except ImportError:  # server-only installs don't need these
    pytest = None

if pytest is not None:
    THIS = sys.modules[__name__]

    @pytest.fixture()
    def client(tmp_path):
        app = create_app(f"sqlite:///{tmp_path/'test.db'}", tmp_path / "certs")
        with TestClient(app) as cl:
            yield cl

    def _payload(n=3, **extra):
        body = {
            "event_name": "Advanced Strategic Innovation Workshop 2026",
            "issuer_name": "Jonathan Patterson", "issuer_title": "Program Director",
            "issue_date": "2026-03-03",
            "recipients": [{"name": f"Person {i}", "email": f"p{i}@example.com"} for i in range(n)],
        }
        body.update(extra)
        return body

    def test_create_job_returns_202_and_job_id(client):
        r = client.post("/jobs", json=_payload(5))
        assert r.status_code == 202
        data = r.json()
        assert data["total"] == 5 and data["job_id"] and data["status_url"] == f"/jobs/{data['job_id']}"

    def test_csv_with_names_only_creates_certificates(client):
        r = client.post(
            "/jobs/csv",
            files={"file": ("names.csv", "name\nAsha Rao\nDiego Martin\n", "text/csv")},
            data={"event_name": "CSV Participation Workshop"},
        )
        assert r.status_code == 202
        status = client.get(r.json()["status_url"]).json()
        assert status["status"] == "completed"
        assert [c["recipient_name"] for c in status["certificates"]] == ["Asha Rao", "Diego Martin"]
        assert all(c["recipient_email"] is None for c in status["certificates"])

    def test_csv_with_names_and_emails_creates_certificates(client):
        r = client.post(
            "/jobs/csv",
            files={"file": ("recipients.csv", "name,email\nAsha Rao,asha@example.com\nDiego Martin,diego@example.com\n", "text/csv")},
            data={"event_name": "CSV Email Workshop", "certificate_type": "Achievement"},
        )
        assert r.status_code == 202
        status = client.get(r.json()["status_url"]).json()
        assert status["status"] == "completed"
        assert [c["recipient_email"] for c in status["certificates"]] == [
            "asha@example.com", "diego@example.com"
        ]

    def test_csv_requires_name_column(client):
        r = client.post(
            "/jobs/csv",
            files={"file": ("bad.csv", "email\nasha@example.com\n", "text/csv")},
            data={"event_name": "Invalid CSV"},
        )
        assert r.status_code == 422

    @pytest.mark.parametrize("bad", [
        {"recipients": []},                                  # nothing to generate
        {"recipients": [{"name": "x"}] * (MAX_RECIPIENTS + 1)},  # too many
        {"event_name": ""},                                  # missing event
        {"issue_date": "not-a-date"},
    ])
    def test_request_level_validation_rejects_with_422(client, bad):
        r = client.post("/jobs", json=_payload(2, **bad))
        assert r.status_code == 422

    def test_invalid_recipients_are_failed_but_valid_ones_proceed(client):
        body = _payload(0, recipients=[
            {"name": "Good One", "email": "good@example.com"},
            {"name": "   "},                                  # blank name
            {"name": "Bad Mail", "email": "not-an-email"},    # bad email
            "just a string",                                  # wrong shape
            {"name": "Good Two"},
        ])
        job = client.post("/jobs", json=body).json()
        status = client.get(job["status_url"]).json()
        assert status["status"] == "completed_with_errors"
        assert (status["succeeded"], status["failed"], status["total"]) == (2, 3, 5)
        by_pos = {c["position"]: c for c in status["certificates"]}
        assert by_pos[0]["status"] == "success" and by_pos[4]["status"] == "success"
        for p in (1, 2, 3):
            assert by_pos[p]["status"] == "failed" and by_pos[p]["error"].startswith("Invalid recipient")

    def test_all_invalid_job_is_failed_immediately(client):
        job = client.post("/jobs", json=_payload(0, recipients=[{"name": ""}, {}])).json()
        assert client.get(job["status_url"]).json()["status"] == "failed"

    def test_certificate_is_generated_as_valid_one_page_pdf(client, monkeypatch):
        from reportlab import rl_config
        monkeypatch.setattr(rl_config, "pageCompression", 0)  # keep text greppable
        monkeypatch.setattr(rl_config, "useA85", 0)
        job = client.post("/jobs", json=_payload(0, recipients=[{"name": "Harumi Kobayashi"}])).json()
        item = client.get(job["status_url"]).json()["certificates"][0]
        pdf = client.get(item["download_url"])
        assert pdf.status_code == 200 and pdf.headers["content-type"] == "application/pdf"
        assert pdf.content.startswith(b"%PDF") and pdf.content.count(b"/Type /Page\n") == 1
        assert b"Harumi Kobayashi" in pdf.content and b"Jonathan Patterson" in pdf.content
        assert b"3rd day of March, 2026" in pdf.content

    @pytest.mark.parametrize("n", [5, 10, 15, 50, 100, 200])
    def test_supported_bulk_sizes_complete(client, n):
        job = client.post("/jobs", json=_payload(n)).json()
        st = client.get(job["status_url"]).json()
        assert st["status"] == "completed" and st["succeeded"] == n and st["progress_percent"] == 100.0

    def test_status_and_progress_fields(client):
        job = client.post("/jobs", json=_payload(4)).json()
        st = client.get(f"/jobs/{job['job_id']}").json()
        assert st["pending"] == 0 and st["completed_at"] and len(st["certificates"]) == 4
        assert client.get("/jobs/doesnotexist").status_code == 404

    def test_single_certificate_render_failure_does_not_stop_the_job(client, monkeypatch):
        real = THIS.render_certificate

        def flaky(path, **kw):
            if kw["name"] == "Person 1":
                raise RuntimeError("disk exploded")
            return real(path, **kw)

        monkeypatch.setattr(THIS, "render_certificate", flaky)
        job = client.post("/jobs", json=_payload(3)).json()
        st = client.get(job["status_url"]).json()
        assert st["status"] == "completed_with_errors" and (st["succeeded"], st["failed"]) == (2, 1)
        bad = next(c for c in st["certificates"] if c["status"] == "failed")
        assert bad["recipient_name"] == "Person 1" and "disk exploded" in bad["error"] and bad["download_url"] is None
        assert client.get(f"/jobs/{job['job_id']}/certificates/{bad['certificate_id']}").status_code == 409

    def test_unsupported_characters_fail_only_that_certificate(client, monkeypatch):
        monkeypatch.setattr(THIS, "_get_unicode_fonts", lambda: None)  # simulate a server without Unicode font
        job = client.post("/jobs", json=_payload(0, recipients=[{"name": "Ana"}, {"name": "रोहन"}])).json()
        st = client.get(job["status_url"]).json()
        assert [c["status"] for c in st["certificates"]] == ["success", "failed"]

    def test_retrieve_all_as_zip_only_contains_successes(client):
        body = _payload(0, recipients=[{"name": "A One"}, {"name": ""}, {"name": "B Two"}])
        job = client.post("/jobs", json=body).json()
        r = client.get(f"/jobs/{job['job_id']}/download")
        assert r.status_code == 200 and r.headers["content-type"] == "application/zip"
        z = zipfile.ZipFile(io.BytesIO(r.content))
        assert sorted(z.namelist()) == ["001_A_One.pdf", "003_B_Two.pdf"]
        assert all(z.read(n).startswith(b"%PDF") for n in z.namelist())

    def test_certificate_of_other_job_is_not_accessible(client):
        a = client.post("/jobs", json=_payload(1)).json()
        b = client.post("/jobs", json=_payload(1)).json()
        cert_a = client.get(a["status_url"]).json()["certificates"][0]["certificate_id"]
        assert client.get(f"/jobs/{b['job_id']}/certificates/{cert_a}").status_code == 404


# ════════════════════════════════════════════════════════════════════════════
# 7. CLI
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "serve"
    if cmd == "serve":
        import uvicorn
        uvicorn.run(create_app(), host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")))
    elif cmd == "test":
        sys.exit(pytest.main(["-v", "-p", "no:cacheprovider", __file__]))
    elif cmd == "readme":
        print(__doc__)
    else:
        print("usage: python app.py [serve|test|readme]")