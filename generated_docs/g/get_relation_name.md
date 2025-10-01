# get_relation_name

## Location
[src/backend/utils/adt/ruleutils.c:12803-12822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12803-L12822)

## Overview
A utility function that retrieves the unqualified name of a relation by its OID, with strict error handling that throws an error if the relation is not found.

## Definition

```c
static char *
get_relation_name(Oid relid)
```
## Detailed Description
This function serves as a wrapper around the lower-level get_rel_name() function, providing more robust error handling. While get_rel_name() returns NULL for invalid OIDs, this function throws an ERROR instead, ensuring that calling code doesn't need to handle NULL returns. This makes it suitable for use in contexts where the relation is expected to exist, and a missing relation indicates a serious problem that should halt execution.

## Parameters / Member Variables
- `relid`: The OID of the relation whose name should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_name](get_rel_name.md)
  - elog
- Called from (representative examples):
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [pg_get_statisticsobj_worker](../p/pg_get_statisticsobj_worker.md)
  - [pg_get_constraintdef_worker](../p/pg_get_constraintdef_worker.md)
  - [get_rte_alias](get_rte_alias.md)
  - [NameHashEntry](../N/NameHashEntry.md) structure usage

## Notes and Other Information
- This is a static function local to ruleutils.c
- Provides fail-fast behavior for relation lookups where the relation must exist
- Used primarily in SQL rule generation and formatting contexts
- The returned string is allocated by the underlying get_rel_name() function
- Part of PostgreSQL's defensive programming practices for system catalog lookups

## Simplified Source

```c
static char *
get_relation_name(Oid relid)
{
    char *relname = get_rel_name(relid);

    if (!relname)
        elog(ERROR, "cache lookup failed for relation %u", relid);

    return relname;
}
```