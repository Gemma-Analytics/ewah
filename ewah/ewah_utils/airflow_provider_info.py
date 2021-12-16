from ewah.hooks import connection_types


def get_provider_info():
    return {
        "package-name": "ewah",
        "name": "ewah",
        "description": "",
        "connection_types": connection_types,
        "versions": ["1.0.0"],
    }
