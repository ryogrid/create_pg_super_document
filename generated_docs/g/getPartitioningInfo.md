# getPartitioningInfo

## Location
[src/bin/pg_dump/pg_dump.c:7373-7432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7373-L7432)

## Overview
Identifies partitioned tables with "unsafe" partitioning schemes that require load-via-partition-root mode during pg_dump operations, specifically focusing on hash partitioning on enum columns.

## Definition

```c
enum_ops
	 * appears among the partition opclasses.  We needn't check partstrat.
	 *
	 * Note that this query may well retrieve info about tables we aren't
	 * going to dump and hence have no lock on.  That's okay since we need not
	 * invoke any unsafe server-side functions.
	 */
	appendPQExpBufferStr(query,
						 "SELECT partrelid FROM pg_partitioned_table WHERE\n"
						 "(SELECT c.oid FROM pg_opclass c JOIN pg_am a "
						 "ON c.opcmethod = a.oid\n"
						 "WHERE opcname = 'enum_ops' "
						 "AND opcnamespace = 'pg_catalog'::regnamespace "
						 "AND amname = 'hash') = ANY(partclass)");
```
## Detailed Description
The getPartitioningInfo function analyzes all partitioned tables in the database to identify those with partitioning schemes that are considered "unsafe" for normal dump and restore operations. Currently, the primary concern is hash partitioning on enum columns, where hash codes depend on enum value OIDs that won't be preserved across dump-and-reload cycles. The function queries pg_partitioned_table and related catalogs to find tables using hash partitioning with enum_ops operator classes, then marks these tables as requiring special handling during data loading. This ensures data integrity during backup and restore operations by forcing the use of partition root tables for data insertion.

## Parameters / Member Variables
- : Archive pointer containing dump configuration and remote version information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - atooid
  - [TableInfo](../T/TableInfo.md) (structure type)
  - [findTableByOid](../f/findTableByOid.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- Only processes databases with PostgreSQL version 11.0000 or higher (hash partitioning introduction)
- Skips processing entirely for schema-only dumps since data loading isn't involved
- The function examines all partitioned tables, not just those being dumped, to handle parent-child relationships correctly
- Sets the unsafe_partitions flag on affected TableInfo structures to influence later dump behavior
- The safety check is specifically for hash partitioning with enum_ops operator class in the pg_catalog namespace
- Handles tables that may not have locks since it only queries catalog information without invoking server-side functions

## Simplified Source

```c
void getPartitioningInfo(Archive *fout)
{
    PQExpBuffer query;
    PGresult   *res;
    int         ntups;

    // Skip if hash partitioning not available (pre-v11)
    if (fout->remoteVersion < 110000)
        return;

    // Skip for schema-only dumps
    if (fout->dopt->schemaOnly)
        return;

    query = createPQExpBuffer();

    // Find tables with unsafe hash partitioning on enum columns
    // Hash codes depend on enum OIDs which aren't preserved across dump/reload
    appendPQExpBufferStr(query,
        "SELECT partrelid FROM pg_partitioned_table WHERE "
        "(SELECT c.oid FROM pg_opclass c JOIN pg_am a "
        "ON c.opcmethod = a.oid "
        "WHERE opcname = 'enum_ops' "
        "AND opcnamespace = 'pg_catalog'::regnamespace "
        "AND amname = 'hash') = ANY(partclass)");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Mark each unsafe table for special handling during dump
    for (int i = 0; i < ntups; i++)
    {
        Oid         tabrelid = atooid(PQgetvalue(res, i, 0));
        TableInfo  *tbinfo = findTableByOid(tabrelid);

        if (tbinfo == NULL)
            pg_fatal("table OID %u not found", tabrelid);

        tbinfo->unsafe_partitions = true;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```