import pendulum

from work_planner.models import Workplan

not_expired = (
    pendulum.now("UTC").timestamp() < Workplan.expires_utc.to_timestamp()
) | (Workplan.expires_utc.is_null())

expired = pendulum.now("UTC").timestamp() >= Workplan.expires_utc.to_timestamp()
