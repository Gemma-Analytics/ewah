from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
from sqlalchemy.exc import IntegrityError

import os

env = os.environ
if env.get('EWAH_AIRFLOW_USER_SET'):
    username = env.get('EWAH_AIRFLOW_USER_USER', 'ewah')
    password = env.get('EWAH_AIRFLOW_USER_PASSWORD', 'ewah')
    email = env.get('EWAH_AIRFLOW_USER_EMAIL', 'ewah@ewah.com')

    try:
        user = PasswordUser(models.User())
        user.username = username
        user.email = email
        user.password = password
        user.superuser = True
        session = settings.Session()
        session.add(user)
        session.commit()
        session.close()
        exit()
        print('Added Admin user {0}!'.format(username))
    except IntegrityError:
        print('User {0} already exists!'.format(username))
