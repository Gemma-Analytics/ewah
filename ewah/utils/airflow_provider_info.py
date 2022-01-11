from ewah.hooks import connection_types


def get_provider_info():
    return {
        "package-name": "ewah",
        "name": "EWAH",
        "description": "ELT With Airflow Helper - EWAH. Make EL easy for the world.",
        "connection-types": connection_types,
        "version": "1.0.0",
    }
