from airflow.utils.db import create_session
from airflow.models import Variable


with create_session() as session:
    af_var = {var.key: var.val for var in session.query(Variable)}

print(af_var)
