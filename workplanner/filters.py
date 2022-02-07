import pendulum

from pbm_helper.workplanner.enums import Statuses
from models import Workplan

not_expired = (pendulum.now().timestamp() < Workplan.expires_utc.to_timestamp()) | (
    Workplan.expires_utc.is_null()
)

expired = pendulum.now().timestamp() >= Workplan.expires_utc.to_timestamp()

for_executed = (Workplan.status.in_(Statuses.for_executed), not_expired)
