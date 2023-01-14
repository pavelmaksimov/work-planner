from uuid import UUID

from fastapi import Depends, APIRouter
from fastapi.responses import ORJSONResponse
from script_master_helper.workplanner import schemas
from sqlalchemy.orm import Session

from workplanner import errors, service, crud, models
from workplanner.database import get_db

API_VERSION = "1.0.0"

router = APIRouter()


@router.post("/workplan/list", response_class=ORJSONResponse)
def list_resource(workplan_query: schemas.WorkplanQuery, db: Session = Depends(get_db)):
    query = crud.QueryFilter(schema=workplan_query).get_query_with_filter()
    result = db.scalars(query)
    workplans = schemas.Workplan.list_from_orm(result)
    response = schemas.ResponseGeneric(data=workplans)

    return response


@router.post("/workplan/update", response_class=ORJSONResponse)
def update_resource(
    workplan_update: schemas.WorkplanUpdate, db: Session = Depends(get_db)
):
    item = service.update(db, workplan_update)

    if not item:
        raise errors.get_404_exception(
            f"id={workplan_update.id}, name={workplan_update.name}, worktime={workplan_update.worktime_utc}"
        )

    workplan = schemas.Workplan.from_orm(item)

    return schemas.ResponseGeneric(data=workplan)


@router.post("/workplan/update/list", response_class=ORJSONResponse)
def update_list_resource(
    workplans: schemas.WorkplanListGeneric[schemas.WorkplanUpdate],
    db: Session = Depends(get_db),
):
    count = len(service.many_update(db, workplans.workplans))
    return schemas.ResponseGeneric(data=schemas.Affected(count=count))


@router.post("/workplan/generate/list", response_class=ORJSONResponse)
def generate_resource(schema: schemas.GenerateWorkplans, db: Session = Depends(get_db)):
    iterator = service.generate_workplans(db, schema)
    workplans = schemas.Workplan.list_from_orm(iterator)

    return schemas.ResponseGeneric(data=workplans)


@router.post("/workplan/generate/child/list", response_class=ORJSONResponse)
def generate_resource(
    schema: schemas.GenerateChildWorkplans, db: Session = Depends(get_db)
):
    iterator = service.generate_child_workplans(db, schema)
    workplans = schemas.Workplan.list_from_orm(iterator)

    return schemas.ResponseGeneric(data=workplans)


@router.get("/workplan/execute/{name}/list", response_class=ORJSONResponse)
def execute_list_resource(name: str, db: Session = Depends(get_db)):
    iterator = service.execute_list(db, name)
    workplans = schemas.Workplan.list_from_orm(iterator)

    return schemas.ResponseGeneric(data=workplans)


@router.post("/workplan/delete", response_class=ORJSONResponse)
def delete_resource(
    workplan_filter: schemas.WorkplanQuery, db: Session = Depends(get_db)
):
    count = len(db.scalars(crud.delete(filter_schema=workplan_filter)).all())

    return schemas.ResponseGeneric(data=schemas.Affected(count=count))


@router.post("/workplan/count", response_class=ORJSONResponse)
def count_resource(
    workplan_filter: schemas.WorkplanQuery, db: Session = Depends(get_db)
):
    query = crud.QueryFilter(schema=workplan_filter).get_query_with_filter()
    count = len(db.scalars(query).all())

    return schemas.ResponseGeneric(data=schemas.Affected(count=count))


@router.post("/workplan/count/by/list", response_class=ORJSONResponse)
def count_by_resource(
    workplan_fields: schemas.WorkplanFields, db: Session = Depends(get_db)
):
    fields = [getattr(models.Workplan, name) for name in workplan_fields.field_names]
    query = crud.count_by(*fields)
    data = db.execute(query).mappings()
    return schemas.ResponseGeneric(data=data)


@router.post("/workplan/reset", response_class=ORJSONResponse)
def reset_resource(pk: schemas.WorkplanPK, db: Session = Depends(get_db)):
    query = crud.reset(pk.name, [pk.worktime_utc])
    item = db.scalar(query)
    data = schemas.Workplan.from_orm(item)

    return schemas.ResponseGeneric(data=data)


@router.get("/workplan/{id}/replay", response_class=ORJSONResponse)
def run_resource(id: UUID, db: Session = Depends(get_db)):
    wp = service.run(db, id)
    if not wp:
        raise errors.get_404_exception(f"{id=}")

    return schemas.ResponseGeneric(data=schemas.Workplan.from_orm(wp))
