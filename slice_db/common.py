import asyncpg


async def setup_connection(conn: asyncpg.Connection):
    await conn.execute("SET lock_timeout = 0")
    await conn.execute("SET row_security = off")
    await conn.execute("SET statement_timeout = 0")

    await conn.execute("SELECT pg_catalog.set_config('search_path', '', false)")
