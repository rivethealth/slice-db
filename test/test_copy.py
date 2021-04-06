import slice_db.pg.copy


def test_parse():
    slice_db.pg.copy.COPY_FORMAT.parse_field("a") == "a"
    slice_db.pg.copy.COPY_FORMAT.parse_field(r"\N") == None
    slice_db.pg.copy.COPY_FORMAT.parse_field(r"a\nb") == "a\nb"


def test_serialze():
    slice_db.pg.copy.COPY_FORMAT.serialize_field("a") == "a"
    slice_db.pg.copy.COPY_FORMAT.serialize_field(None) == r"\N"
    slice_db.pg.copy.COPY_FORMAT.serialize_field("a\nb") == r"a\nb"
