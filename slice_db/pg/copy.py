import typing

Field = typing.Optional[str]
RawRow = typing.List[str]


class CopyFormat:
    def parse_field(self, text: str) -> Field:
        if text == r"\N":
            return None

        if "\\" not in text:
            return text

        i = 0
        result = ""
        while i < len(text):
            try:
                j = text.index("\\", i)
            except ValueError:
                result += text[i:]
                break
            result += text[i:j]
            if text[j + 1] == "\\":
                result += "\\"
            elif text[j + 1] == "b":
                result += "\b"
            elif text[j + 1] == "f":
                result += "\f"
            elif text[j + 1] == "n":
                result += "\n"
            elif text[j + 1] == "r":
                result += "\r"
            elif text[j + 1] == "t":
                result += "\t"
            elif text[j + 1] == "v":
                result += "\v"
            i = j + 2

        return result

    def parse_raw_row(self, text: str) -> RawRow:
        return text.split("\t")

    def serialize_field(self, field: Field) -> str:
        if field is None:
            return r"\N"

        return (
            field.replace("\\", "\\\\")
            .replace("\b", "\\b")
            .replace("\f", "\\f")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
            .replace("\v", "\\t")
        )

    def serialize_raw_row(self, row: RawRow) -> str:
        return "\t".join(row)


COPY_FORMAT = CopyFormat()
