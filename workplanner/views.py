from fastapi import Depends, FastAPI

from pbm_helper.workplanner import schemas
from workplanner.database import db
from workplanner import errors, service, crud
from workplanner import models
from crud import QueryFilter
from service import clear_statuses_of_lost_items

API_VERSION = "1.0.0"

clear_statuses_of_lost_items()


app = FastAPI(version=API_VERSION, title="WorkPlanner")


@app.on_event("startup")
async def startup():
    await db.connect()


@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()


@app.post("/workplan/list", dependencies=[Depends(get_db)])
def list_view(workplan_query: schemas.WorkplanQuery):
    query = crud.QueryFilter(schema=workplan_query).get_select_query()
    items = models.Workplan.items_to_pydantic(query)
    response = schemas.ResponseGeneric(data=items)

    return response


@app.post("/workplan/update", dependencies=[Depends(get_db)])
def update_view(workplan_update: schemas.WorkplanUpdate):
    item = crud.update(workplan_update)

    if item:
        workplan = item.to_pydantic()
        response = schemas.ResponseGeneric(data=workplan)
    else:
        response = schemas.ResponseGeneric(error=errors.not_found.copy(update={"detail": {"id": workplan_update.id}}))

    return response


@app.post("/workplan/update/list", dependencies=[Depends(get_db)])
def update_list_view(
    workplans: schemas.WorkplanListGeneric[schemas.WorkplanUpdate],
):
    workplan_list = workplans.dict(exclude_unset=True)["workplans"]
    count = crud.many_update(workplan_list)
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


@app.get("/workplan/execute/{name}/list", dependencies=[Depends(get_db)])
def execute_list_view(name: str):
    query = service.execute_list(name)
    workplans = models.Workplan.items_to_pydantic(query)
    response = schemas.ResponseGeneric(data=workplans)
    return response


@app.post("/workplan/delete", dependencies=[Depends(get_db)])
def delete_view(workplan_filter: schemas.WorkplanQuery):
    count = crud.delete(query_filter=workplan_filter)
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response


@app.post("/workplan/count", dependencies=[Depends(get_db)])
def count_view(workplan_filter: schemas.WorkplanQuery):
    count = QueryFilter(schema=workplan_filter).count()
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response


@app.post("/workplan/count/by/list", dependencies=[Depends(get_db)])
def count_by_view(workplan_fields: schemas.WorkplanFields):
    fields = [getattr(models.Workplan, name) for name in workplan_fields.field_names]
    query = crud.count_by(*fields)
    workplans = list(query.dicts())
    response = schemas.ResponseGeneric(data=workplans)
    return response


@app.post("/workplan/recreate", dependencies=[Depends(get_db)])
def recreate_view(pk: schemas.WorkplanPK):
    item = crud.recreate(pk.name, pk.worktime_utc)
    return schemas.ResponseGeneric(data=item.to_pydantic())


@app.get("/workplan/{id}/replay", dependencies=[Depends(get_db)])
def replay_view(id: str):
    count = crud.replay_by_id(id)
    response = schemas.ResponseGeneric(data=schemas.Affected(count=count))
    return response
