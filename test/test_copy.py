import slice_db.pg.copy


def test_parse():
    assert slice_db.pg.copy.COPY_FORMAT.parse_field("a") == "a"
    assert slice_db.pg.copy.COPY_FORMAT.parse_field(r"\N") == None
    assert slice_db.pg.copy.COPY_FORMAT.parse_field(r"a\nb") == "a\nb"


def test_serialize():
    assert slice_db.pg.copy.COPY_FORMAT.serialize_field("a") == "a"
    assert slice_db.pg.copy.COPY_FORMAT.serialize_field(None) == r"\N"
    assert slice_db.pg.copy.COPY_FORMAT.serialize_field("a\nb") == r"a\nb"
