import dataclasses
import enum
import typing


class _State(enum.Enum):
    COMMENT = enum.auto()
    COMMENT_START = enum.auto()
    IDENTIFIER = enum.auto()
    OTHER = enum.auto()
    STRING = enum.auto()
    STRING_QUOTE = enum.auto()


def parse_statements(text: str):
    """
    Split into statements
    """
    state = _State.OTHER
    start = 0
    i = 0
    while True:
        try:
            c = text[i]
        except IndexError:
            c = None
        if state == _State.COMMENT:
            if c == "\n":
                state = _State.OTHER
                i += 1
            else:
                i += 1
        elif state == _State.COMMENT_START:
            if c == "-":
                state = _State.COMMENT
                i += 1
            else:
                state = _State.OTHER
        elif state == _State.IDENTIFIER:
            if c == '"':
                state = _State.OTHER
                i += 1
            elif c is None:
                raise Exception("Broken identifier")
            else:
                i += 1
        elif state == _State.OTHER:
            if c == "-":
                state = _State.COMMENT_START
                i += 1
            elif c == "'":
                state = _State.STRING
                i += 1
            elif c == '"':
                state = _State.IDENTIFIER
                i += 1
            elif c == ";":
                yield text[start:i]
                i += 1
                start = i
            elif c is None:
                if start < i:
                    yield text[start:i]
                break
            else:
                i += 1
        elif state == _State.STRING:
            if c == "'":
                state = _State.STRING_QUOTE
                i += 1
            elif c is None:
                raise Exception("Broken string")
            else:
                i += 1
        elif state == _State.STRING_QUOTE:
            if c == "'":
                state = _State.STRING
                i += 1
            else:
                state = _State.OTHER
