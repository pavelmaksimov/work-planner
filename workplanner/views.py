from fastapi import Depends, FastAPI

from workplanner import database
from workplanner import models
from workplanner import schemas, errors, service, crud
from workplanner.service import clear_statuses_of_lost_items

API_VERSION = "1.0.0"

database.db.connect()
database.db.create_tables([models.Workplan])
clear_statuses_of_lost_items()
database.db.close()


async def reset_db_state():
    database.db._state._state.set(database.db_state_default.copy())
    database.db._state.reset()


def get_db(db_state=Depends(reset_db_state)):  # pylint: disable=W0613
    try:
        database.db.connect()
        yield
    finally:
        if not database.db.is_closed():
            database.db.close()


app = FastAPI(version=API_VERSION, title="WorkPlanner")


@app.post("/workplan/list", dependencies=[Depends(get_db)])
def list_view(workplan_filter: schemas.WorkplanQueryFilter):
    items = workplan_filter.get_as_pydantic()
    response = schemas.ResponseGeneric(data=items)

    return response


@app.post("/workplan/update", dependencies=[Depends(get_db)])
def update_view(workplan_update: schemas.WorkplanUpdate):
    workplan = workplan_update.save()
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


@app.post("/workplan/generate/list", dependencies=[Depends(get_db)])
def generate_view(
    data: schemas.GenerateWorkplans,
):
    dct = data.dict(exclude_unset=True)
    query = service.generate_workplans(**dct)
    workplans = models.Workplan.items_to_pydantic(query)
    response = schemas.ResponseGeneric(data=workplans)
    return response


@app.post("/workplan/execute/list", dependencies=[Depends(get_db)])
def execute_list_view(workplan_name: schemas.WorkplanName):
    query = service.execute_list(workplan_name.name)
    workplans = models.Workplan.items_to_pydantic(query)
    response = schemas.ResponseGeneric(data=workplans)
    return response


@app.post("/workplan/delete", dependencies=[Depends(get_db)])
def delete_view(workplan_filter: schemas.WorkplanQueryFilter):
    count = crud.delete(query_filter=workplan_filter)
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response


@app.post("/workplan/count", dependencies=[Depends(get_db)])
def count_view(workplan_filter: schemas.WorkplanQueryFilter):
    count = workplan_filter.count()
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response


@app.post("/workplan/count/by/list", dependencies=[Depends(get_db)])
def count_by_view(workplan_fields: schemas.WorkplanFields):
    model_fields = workplan_fields.iter_model_fields()
    query = crud.count_by(*model_fields)
    workplans = list(query.dicts())
    response = schemas.ResponseGeneric(data=workplans)
    return response


@app.post("/workplan/recreate", dependencies=[Depends(get_db)])
def recreate_view(pk: schemas.WorkplanPK):
    item = crud.recreate(pk.name, pk.worktime_utc)
    return schemas.ResponseGeneric(data=item.to_pydantic())


@app.post("/workplan/replay", dependencies=[Depends(get_db)])
def replay_view(many_pk: schemas.WorkplanManyPK):
    count = crud.replay(many_pk.name, many_pk.worktime_utc_list)
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response
