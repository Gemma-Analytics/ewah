from ewah.hooks import hook_class_names, connection_types


def get_provider_info():
    return {
        "package-name": "ewah",
        "name": "ewah",
        "description": "",
        "hook-class-names": hook_class_names,
        # hook-class-names is deprecated from 2.2.0 and will be removed in airflow 3.0
        # use connection-types instead, keep hook-class-names for backward compatibility
        "connection_types": connection_types,
        "versions": ["1.0.0"],
    }
