from ewah.hooks import hook_class_names

def get_provider_info():
    return {
        'package-name': 'ewah',
        'name': 'ewah',
        "description": "",
        "hook-class-names": hook_class_names,
        'versions': ["0.3.0"],
    }
