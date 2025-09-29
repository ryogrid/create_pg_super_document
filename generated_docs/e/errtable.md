# errtable

## Location
[src/backend/utils/cache/relcache.c:5957-5973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5957-L5973)

## Overview
Stores schema name and table name of a relation within the current error context for enhanced error reporting.

## Definition
```c
int errtable(Relation rel)
```

## Detailed Description
This function is a utility for error reporting that captures relation-specific context information and stores it in the current error data structure. It extracts the schema name and table name from the provided relation and adds them to the error context using PostgreSQL's error reporting framework. This allows error messages to include specific table and schema information, making errors more informative and easier to debug.

The function is designed to be used within ereport() calls to provide contextual information about which table was involved in an error condition. It follows PostgreSQL's error reporting conventions by using standard diagnostic fields (PG_DIAG_SCHEMA_NAME and PG_DIAG_TABLE_NAME).

## Parameters / Member Variables
- `rel`: The relation (table) for which to store schema and table name information in the error context

## Dependencies
- Functions called/Symbols referenced:
  - [err_generic_string](err_generic_string.md) (with PG_DIAG_SCHEMA_NAME)
  - [get_namespace_name](../g/get_namespace_name.md)
  - RelationGetNamespace
  - [err_generic_string](err_generic_string.md) (with PG_DIAG_TABLE_NAME)
  - RelationGetRelationName
- Called from (representative examples):
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [ATPrepChangePersistence](../A/ATPrepChangePersistence.md)
  - [ExecPartitionCheckEmitError](../E/ExecPartitionCheckEmitError.md)
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [check_default_partition_contents](../c/check_default_partition_contents.md)
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)
  - [errtablecolname](errtablecolname.md)
  - [errtableconstraint](errtableconstraint.md)

## Notes and Other Information
- Part of a family of error reporting utility functions located in relcache.c to avoid module layering violations
- The return value (0) does not matter and is ignored by callers
- Designed to be used as part of ereport() chains to enhance error messages with table context
- Uses PostgreSQL's standard diagnostic message fields for structured error reporting

## Simplified Source

```c
int errtable(Relation rel) {
    // Store schema name in error context
    err_generic_string(PG_DIAG_SCHEMA_NAME,
                      get_namespace_name(RelationGetNamespace(rel)));

    // Store table name in error context
    err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(rel));

    return 0;  // Return value doesn't matter
}
```