import datetime as dt
import uuid

import pendulum
import sqlalchemy as sa
from script_master_helper.workplanner.enums import Statuses
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from workplanner.fields import PendulumDateTime


class Base(DeclarativeBase):
    pass


class Workplan(Base):
    __tablename__ = "workplans"

    name: Mapped[str] = mapped_column(sa.String(100), primary_key=True)
    worktime_utc: Mapped[dt.datetime] = mapped_column(
        PendulumDateTime, primary_key=True
    )
    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid, nullable=False, index=True, unique=True, default=uuid.uuid4
    )
    status: Mapped[str] = mapped_column(
        sa.String(30), index=True, nullable=False, default=Statuses.default
    )
    hash: Mapped[str] = mapped_column(sa.String(30), nullable=True)
    retries: Mapped[int] = mapped_column(default=0, nullable=False)
    info: Mapped[str] = mapped_column(nullable=True)
    data: Mapped[dict] = mapped_column(sa.JSON, default="{}", nullable=False)
    expires_utc: Mapped[dt.datetime] = mapped_column(PendulumDateTime, nullable=True)
    started_utc: Mapped[dt.datetime] = mapped_column(PendulumDateTime, nullable=True)
    finished_utc: Mapped[dt.datetime] = mapped_column(PendulumDateTime, nullable=True)
    created_utc: Mapped[dt.datetime] = mapped_column(
        PendulumDateTime, default=lambda: pendulum.now(), server_default=sa.func.now()
    )
    updated_utc: Mapped[dt.datetime] = mapped_column(
        PendulumDateTime,
        default=lambda: pendulum.now(),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
    )

    @property
    def pk(self):
        return self.name, self.worktime_utc

    @hybrid_property
    def duration(self) -> int | None:
        if self.finished_utc and self.started_utc:
            return int((self.finished_utc - self.started_utc).total_seconds())
