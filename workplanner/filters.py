import pendulum

from workplanner.enums import Statuses
from workplanner.models import Workplan

not_expired = (pendulum.now().timestamp() < Workplan.expires_utc.to_timestamp()) | (
    Workplan.expires_utc.is_null()
)

expired = pendulum.now().timestamp() >= Workplan.expires_utc.to_timestamp()

for_executed = (Workplan.status == Statuses.add, not_expired)
