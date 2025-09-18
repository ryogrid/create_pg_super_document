# binary_upgrade_set_type_oids_by_type_oid

## Location
[src/bin/pg_dump/pg_dump.c:5376-5460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5376-L5460)

## Overview
Generates binary upgrade commands to preserve type OIDs and their associated array and multirange type OIDs during PostgreSQL binary upgrades.

## Definition
```c
static void binary_upgrade_set_type_oids_by_type_oid(Archive *fout,
                                                     PQExpBuffer upgrade_buffer,
                                                     Oid pg_type_oid,
                                                     bool force_array_type,
                                                     bool include_multirange_type)
```

## Detailed Description
This function is a core component of PostgreSQL's binary upgrade infrastructure. It ensures that type OIDs are preserved when upgrading between major PostgreSQL versions by generating the necessary SQL statements to pre-set OIDs before type creation. The function handles three types of OID preservation:

1. **Base type OID**: Always preserved using `binary_upgrade_set_next_pg_type_oid`
2. **Array type OID**: Preserved if it exists, or allocated if `force_array_type` is true
3. **Multirange type OID**: For PostgreSQL 14+, preserves multirange types and their arrays for range types

The function queries the system catalogs to determine existing OIDs and generates appropriate `SELECT` statements that call binary upgrade helper functions. For newer features not present in older versions, it allocates new OIDs using `get_next_possible_free_pg_type_oid`.

## Parameters / Member Variables
- `fout`: Archive structure containing database connection and version information
- `upgrade_buffer`: PQExpBuffer where the generated SQL statements are appended
- `pg_type_oid`: The OID of the primary type being processed
- `force_array_type`: If true, ensures an array type OID is assigned even if none existed originally
- `include_multirange_type`: If true, handles multirange type OID preservation for range types

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer/destroyPQExpBuffer (buffer management)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)/appendPQExpBufferStr (SQL statement construction)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (database queries)
  - atooid (string to OID conversion)
  - [get_next_possible_free_pg_type_oid](../g/get_next_possible_free_pg_type_oid.md) (free OID allocation)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted buffer writing)
  - [PQgetvalue](../P/PQgetvalue.md)/PQfnumber/PQclear (query result handling)
  - OidIsValid (OID validation)
- Called from (representative examples):
  - [binary_upgrade_set_type_oids_by_rel](binary_upgrade_set_type_oids_by_rel.md) (src/bin/pg_dump/pg_dump.c:5468)
  - [dumpEnumType](../d/dumpEnumType.md) (src/bin/pg_dump/pg_dump.c:11000)
  - [dumpRangeType](../d/dumpRangeType.md) (src/bin/pg_dump/pg_dump.c:11154)
  - [dumpBaseType](../d/dumpBaseType.md) (src/bin/pg_dump/pg_dump.c:11435)
  - [dumpDomain](../d/dumpDomain.md) (src/bin/pg_dump/pg_dump.c:11619)
  - [dumpCompositeType](../d/dumpCompositeType.md) (src/bin/pg_dump/pg_dump.c:11852)

## Notes and Other Information
- Essential for maintaining object identity across PostgreSQL major version upgrades
- Version-aware: handles multirange types only for PostgreSQL 14+ (fout->remoteVersion >= 140000)
- Generates SQL comments in the upgrade script for clarity and debugging
- Uses temporary query buffer that is properly cleaned up after use
- The `force_array_type` parameter addresses cases where array types weren't created in older versions but are required in newer ones
- Multirange support was introduced in PostgreSQL 14, so the function handles version compatibility by allocating new OIDs for older versions being upgraded