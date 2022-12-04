import datetime as dt

from script_master_helper.workplanner.enums import Statuses

from workplanner.models import Workplan

not_expired = (dt.datetime.utcnow() < Workplan.expires_utc) | (
    Workplan.expires_utc.is_(None)
)

expired = dt.datetime.utcnow() >= Workplan.expires_utc

for_executed = (Workplan.status.in_(Statuses.for_executed), not_expired)
