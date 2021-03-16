import dataclasses
import re
import typing

header_re = re.compile(
    r"-- (?:Data for )?Name: (?P<name>[^(;]+)(?:\([^;]*\))?; Type: (?P<type>[^;]+); Schema: (?P<schema>[^;]+);"
)


@dataclasses.dataclass
class PgDumpSections:
    postdata: str
    predata: str


@dataclasses.dataclass
class PgObject:
    name: typing.Optional[str]
    schema: typing.Optional[str]
    type: typing.Optional[str]
    lines: typing.List[str]


def objects(lines):
    prefix = None
    object = PgObject(name=None, schema=None, type=None, lines=[])
    for line in lines:
        if prefix is None:
            if line == "--":
                prefix = line
            else:
                object.lines.append(line)
        else:
            m = header_re.match(line)
            if m is None:
                object.lines.append(prefix)
                prefix = None
                object.lines.append(line)
            else:
                object.lines.append("")
                yield object
                object = PgObject(
                    name=m.group("name"),
                    schema=m.group("schema"),
                    type=m.group("type"),
                    lines=[prefix, line],
                )
                prefix = None
    yield object


def sections(string: str) -> PgDumpSections:
    lines = string.split("\n")
    data = False
    postdata = ""
    predata = ""
    for object in objects(lines):
        print(object)
        data = data or object.type not in (
            None,
            "DEFAULT",
            "SEQUENCE",
            "SEQUENCE OWNED BY",
            "TABLE",
            "VIEW",
        )
        text = "\n".join(object.lines)
        if data:
            postdata += text
        else:
            predata += text
    return PgDumpSections(postdata=postdata, predata=predata)
