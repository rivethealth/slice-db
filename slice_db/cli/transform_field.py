import json
import secrets
import sys

from ..transform import create_transform


def transform_field_main(args):
    transform = create_transform(args.transform, json.loads(args.params))

    if args.pepper is not None:
        pepper = args.pepper.encode("ascii")
    else:
        pepper = secrets.token_bytes(8)

    print(transform.transform(args.field, pepper) or "")
