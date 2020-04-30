
# https://medium.com/@joel.barmettler/how-to-upload-your-python-package-to-pypi-65edc5fe9c56

from distutils.core import setup
setup(
  name = 'ewah',
  packages = ['ewah'],
  version = '0.1.4',
  license='MIT',
  description = 'An ELT with airflow helper module: Ewah',
  author = 'Bijan Soltani',
  author_email = 'bijan.soltani+ewah@gemmaanalytics.com',
  url = 'https://gemmaanalytics.com/',
  download_url = 'https://github.com/Gemma-Analytics/ewah/archive/0.1.4.tar.gz',
  keywords = ['airflow', 'ELT', 'ETL'],
  install_requires=[
          'pyyaml',
          'psycopg2',
          'gspread',
          'pytz',
          'yahoofinancials',
          'apiclient',
          'oauth2client',
          'cx_Oracle',
          'facebook_business',
          'mysql',
          'snowflake',
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
  ],
)
