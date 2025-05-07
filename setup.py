import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as v:
    VERSION = v.read()

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
        "avro",
        "azure-storage-blob>=2.1.0",  # a temporay 2.0.0 bugfix
        "dbt-metabase<1.0",
        "certifi==2025.1.31",  # Workaround for Snowflake certificate errors
        "croniter",
        "cx_Oracle",
        "facebook_business",
        "google-ads>=16.0.0",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "googlemaps",
        "gspread>=3.6",
        "Jinja2",
        "mailchimp3",
        "oauth2client",
        "Office365-REST-Python-Client",
        "openpyxl",
        "protobuf",
        "psycopg2",
        "pyairtable",
        "pymongo",
        "pymssql==2.3.1",  # Version 2.3.2 fails to build wheel due to missing file sqlfront.h
        "pymysql",
        "python-dateutil",
        "pytz",
        "pyyaml",
        "recurly",
        "selenium",
        "simple-salesforce",
        "snowflake-connector-python>=2.3.8",  # 2.3.8 vendored urrlib3 and requests
        "sshtunnel>=0.2.2",
        "stripe",
        "virtualenv",
        "yahoofinancials-gemma-analytics",  # We needed to create our own fork + module to deal with Too Many Requests error
    ],
)
