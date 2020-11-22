# The datatype of extra in the airflow metadata databse is constrained to 500
# characters. Remove this constraint.

import sqlalchemy
import os

engine = sqlalchemy.create_engine(os.environ.get('AIRFLOW__CORE__SQL_ALCHEMY_CONN'), echo=False)
with engine.begin() as conn:
    conn.execute('ALTER TABLE connection ALTER COLUMN extra TYPE TEXT')
