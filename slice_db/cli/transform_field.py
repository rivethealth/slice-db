import secrets
import sys

from ..formats.transform import TransformInstance
from ..transform import Transforms


def transform_field_main(args):
    if args.pepper is not None:
        pepper = args.pepper.encode("ascii")
    else:
        pepper = secrets.token_bytes(8)

    transforms = Transforms(
        {
            name: TransformInstance.from_dict(value)
            for name, value in args.transforms.items()
        },
        pepper,
    )
    transform = transforms.field(args.name)

    result = transform.transform(args.field)
    print(result or "")
