# InhInfo

## Location
[src/bin/pg_dump/pg_dump.h:531-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L531-L532)

## Overview
InhInfo is a simple structure used by pg_dump to temporarily store table inheritance relationships during the dump process.

## Definition
```c
typedef struct _inhInfo
{
    Oid     inhrelid;       /* OID of a child table */
    Oid     inhparent;      /* OID of its parent */
} InhInfo;
```

## Detailed Description
InhInfo represents table inheritance relationships in PostgreSQL's pg_dump utility. Unlike other dump structures, InhInfo is not a DumpableObject but rather a temporary state structure used internally to track parent-child relationships between tables during the schema analysis phase. This information is crucial for properly handling inherited tables and their dependencies during the dump and restore process.

## Parameters / Member Variables
- `inhrelid`: OID of the child table in the inheritance relationship
- `inhparent`: OID of the parent table in the inheritance relationship

## Dependencies
- Functions called/Symbols referenced:
  - None (simple data structure with OID fields only)
- Called from (representative examples):
  - [getInherits](../g/getInherits.md) (src/bin/pg_dump/pg_dump.c:7323, 7337)
  - [flagInhTables](../f/flagInhTables.md) (src/bin/pg_dump/common.c:294)
  - [getSchemaData](../g/getSchemaData.md) (src/bin/pg_dump/common.c:103)

## Notes and Other Information
- This structure is explicitly noted as "not a DumpableObject, just temporary state"
- Used internally by pg_dump to track inheritance hierarchies before creating the final dump objects
- Essential for proper dependency ordering when dumping inherited table structures
- The information stored corresponds to the pg_inherits system catalog
- Helps ensure that parent tables are dumped before their child tables
- Part of the schema analysis phase that occurs before the actual dumping process