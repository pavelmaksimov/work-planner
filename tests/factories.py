import factory
import pendulum
from script_master_helper.workplanner import enums

from tests.conftest import TestSession
from workplanner import models


class WorkplanFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Workplan
        sqlalchemy_session = TestSession
        sqlalchemy_session_persistence = "flush"

    name = factory.Faker("name")
    worktime_utc = factory.Sequence(lambda n: pendulum.now())
    status = enums.Statuses.default

    @classmethod
    def create_many(cls, size, seconds_interval=60, **kwargs) -> list[models.Workplan]:
        worktime = kwargs.pop(models.Workplan.worktime_utc.key, pendulum.now())
        wp_list = []
        name = kwargs.get("name")
        for i in range(size):
            data = {
                **{
                    models.Workplan.worktime_utc.key: worktime.add(
                        seconds=seconds_interval * (i + 1)
                    )
                },
                **kwargs,
            }
            if name:
                data["name"] = name

            wp = cls.create(**data)
            wp_list.append(wp)
            name = wp.name

        return wp_list
