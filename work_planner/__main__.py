import datetime as dt

from fastapi import Depends
from fastapi import FastAPI

from work_planner import database, schemas, crud, errors
from work_planner import models
from work_planner import service

database.db.connect()
database.db.create_tables([models.Workplan])
database.db.close()

app = FastAPI(debug=True, version="1.0.0", title="Work Planner")


async def reset_db_state():
    database.db._state._state.set(database.db_state_default.copy())
    database.db._state.reset()


def get_db(db_state=Depends(reset_db_state)):
    try:
        database.db.connect()
        yield
    finally:
        if not database.db.is_closed():
            database.db.close()


@app.post("/workplan/list", dependencies=[Depends(get_db)])
def get_list_view(workplan_filter: schemas.WorkplanQueryFilter):
    items = workplan_filter.get_as_pydantic()
    response = schemas.ResponseGeneric(data=items)

    return response


@app.post("/workplan/update", dependencies=[Depends(get_db)])
def update_workplan_view(workplan: schemas.WorkplanUpdate):
    workplan = workplan.save()
    if workplan is None:
        response = schemas.ResponseGeneric(error=errors.ObjectNotFound)
    else:
        response = schemas.ResponseGeneric(data=workplan)

    return response


@app.post("/workplan/update/list", dependencies=[Depends(get_db)])
def update_list_view(
    workplans: schemas.WorkplanListGeneric[schemas.WorkplanUpdate],
):
    count = workplans.save()
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response


@app.post("/workplan/create/list", dependencies=[Depends(get_db)])
def create_view(
    data: schemas.CreateWorkplans,
):
    dct = data.dict(exclude_unset=True)
    select = service.get_items_for_execute(**dct)
    workplans = models.Workplan.items_to_pydantic(select)
    response = schemas.ResponseGeneric(data=workplans)
    return response


@app.post("/workplan/execute/list", dependencies=[Depends(get_db)])
def execute_list_view(workplan_name: schemas.WorkplanName):
    select = service.execute_list(workplan_name.name)
    workplans = models.Workplan.items_to_pydantic(select)
    response = schemas.ResponseGeneric(data=workplans)
    return response


@app.post("/workplan/clear", dependencies=[Depends(get_db)])
def clear_view(workplan_filter: schemas.WorkplanQueryFilter):
    count = crud.clear(query_filter=workplan_filter)
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response


@app.post("/workplan/next-worktime", dependencies=[Depends(get_db)])
def next_worktime_view(data: schemas.NextWorktime):
    interval_timedelta = dt.timedelta(seconds=data.total_seconds)
    worktime_utc = service.next_worktime_utc(
        name=data.name, interval_timedelta=interval_timedelta
    )
    worktime_utc_str = worktime_utc.strftime("%Y-%m-%dT%H:%M:%S")
    response = schemas.ResponseGeneric[dict[str, str]](
        data={"worktime_utc": worktime_utc_str}
    )
    return response


@app.post("/workplan/count", dependencies=[Depends(get_db)])
def count_view(workplan_filter: schemas.WorkplanQueryFilter):
    count = workplan_filter.count()
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response


@app.post("/workplan/count/by/list", dependencies=[Depends(get_db)])
def count_by_view(workplan_fields: schemas.WorkplanFields):
    model_fields = workplan_fields.iter_model_fields()
    select = crud.count_by(*model_fields)
    workplans = list(select.dicts())
    response = schemas.ResponseGeneric(data=workplans)
    return response
