import pendulum
from script_master_helper.workplanner.enums import Operators, Statuses
from script_master_helper.workplanner.schemas import WorkplanQuery

from tests.factories import WorkplanFactory
from workplanner import crud
from workplanner.models import Workplan


def test_equal_QueryFilter(session):
    wp = WorkplanFactory()
    WorkplanFactory()
    schema = WorkplanQuery(
        filter=WorkplanQuery.Filter(id=[WorkplanQuery.Value(value=wp.id)])
    )
    query = crud.QueryFilter(schema).get_query_with_filter()

    assert session.scalar(query).id == wp.id


def test_in_QueryFilter(session):
    wp = WorkplanFactory()
    wp2 = WorkplanFactory()
    WorkplanFactory()
    ids = sorted((wp.id, wp2.id))
    schema = WorkplanQuery(
        filter=WorkplanQuery.Filter(
            id=[
                WorkplanQuery.Value(value=ids),
            ]
        )
    )
    query = (
        crud.QueryFilter(schema)
        .get_query_with_filter(columns=[Workplan.id])
        .order_by(Workplan.id)
    )

    assert session.scalars(query).all() == ids


def test_not_in_QueryFilter(session):
    wp = WorkplanFactory()
    wp2 = WorkplanFactory()
    wp3 = WorkplanFactory()
    schema = WorkplanQuery(
        filter=WorkplanQuery.Filter(
            id=[
                WorkplanQuery.Value(value=[wp.id, wp2.id], operator=Operators.not_in),
            ]
        )
    )
    query = crud.QueryFilter(schema).get_query_with_filter(columns=[Workplan.id])

    assert session.scalars(query).all() == [wp3.id]


def test_get_by_id(session):
    wp = WorkplanFactory()
    WorkplanFactory()
    WorkplanFactory()
    return session.scalar(crud.get_by_id(wp.id)) == wp.id


def test_get_by_name(session):
    WorkplanFactory.create_many(5, name="test_get_by_name")
    WorkplanFactory.create_many(3, name="test_get_by_name2")

    count = len(session.scalars(crud.get_by_name("test_get_by_name")).all())

    return count == 5


def test_count_by(session):
    WorkplanFactory.create_many(5, name="test_count_by")
    WorkplanFactory.create_many(5, name="test_count_by2")
    WorkplanFactory.create_many(
        3,
        worktime_utc=pendulum.now().add(days=1),
        name="test_count_by2",
        status=Statuses.queue,
    )

    assert session.execute(crud.count_by(Workplan.status)).mappings().all() == [
        {"status": "ADD", "count": 10},
        {"status": "QUEUE", "count": 3},
    ]
    assert session.execute(
        crud.count_by(Workplan.name, Workplan.status)
    ).mappings().all() == [
        {"name": "test_count_by", "status": "ADD", "count": 5},
        {"name": "test_count_by2", "status": "ADD", "count": 5},
        {"name": "test_count_by2", "status": "QUEUE", "count": 3},
    ]


def test_reset(session):
    wp_list = WorkplanFactory.create_many(
        2,
        name="test_reset",
        info="Test info",
        status=Statuses.queue,
        retries=5,
        started_utc=pendulum.now(),
        finished_utc=pendulum.now(),
    )
    query = crud.reset("test_reset", (i.worktime_utc for i in wp_list))

    for wp in session.execute(query).scalars().all():
        assert wp.info == None
        assert wp.status == Statuses.default
        assert wp.retries == 0
        assert wp.started_utc == None
        assert wp.finished_utc == None
