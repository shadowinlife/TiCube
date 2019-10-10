from app import celery_app
from engine.plan import PlanNode


@celery_app.task(ignore_result=True, name='Cube Build')
def build(plan_node: PlanNode):

    return True
