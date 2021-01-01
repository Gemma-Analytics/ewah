from ewah.hooks import hook_class_names
from ewah import VERSION


def get_provider_info():
    return {
        "package-name": "ewah",
        "name": "ewah",
        "description": "",
        "hook-class-names": hook_class_names,
        "versions": [VERSION],
    }
