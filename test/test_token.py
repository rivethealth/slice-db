import slice_db.pg.token


def test_parse_statements():
    sql = 'CREATE TABLE "a"();--foo;bar\nCREATE TABLE b();'
    statments = list(slice_db.pg.token.parse_statements(sql))
    assert statments == ['CREATE TABLE "a"()', "--foo;bar\nCREATE TABLE b()"]
