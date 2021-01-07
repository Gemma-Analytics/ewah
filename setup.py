import setuptools
from ewah import VERSION

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ewah",
    version=VERSION,
    author="Bijan Soltani",
    author_email="bijan.soltani+ewah@gemmaanalytics.com",
    description="An ELT with airflow helper module: Ewah",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gemmaanalytics.com/",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.6",
    install_requires=[
        "pyyaml",
        "psycopg2",
        "gspread>=3.6",
        "pytz",
        "yahoofinancials",
        "google-api-python-client",
        "oauth2client",
        "cx_Oracle",
        "facebook_business",
        "pymysql",
        # "snowflake-connector-lite", # "snowflake-connector-python==2.0.2",
        "pymongo",
        "googlemaps",
        "sshtunnel>=0.2.2",
        "mailchimp3",
        "google-cloud-bigquery",
        "stripe",
        "azure-storage-blob>=2.1.0",  # a temporay 2.0.0 bugfix
        "simple-salesforce",
    ],
)
