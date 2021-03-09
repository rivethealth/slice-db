import json

import jsonschema

try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources


class PackageJsonValidator:
    def __init__(self, package, name):
        with pkg_resources.open_text(package, name) as f:
            self.json_schema = json.load(f)

    def validate(self, instance):
        return jsonschema.validate(instance, self.json_schema)
