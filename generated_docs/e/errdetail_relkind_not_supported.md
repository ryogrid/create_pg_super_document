# errdetail_relkind_not_supported

## Location
[src/backend/catalog/pg_class.c:24-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_class.c#L24-L52)

## Overview
A utility function that generates appropriate error detail messages for operations that are not supported on specific PostgreSQL relation kinds (tables, indexes, views, etc.).

## Definition

```c
int
errdetail_relkind_not_supported(char relkind)
```
## Detailed Description
This function provides standardized error detail messages for operations that cannot be performed on certain types of database relations. It takes a relation kind character constant and returns an appropriate errdetail() message explaining why the operation is not supported for that specific type of relation. The function serves as a centralized way to provide consistent, user-friendly error messages across PostgreSQL's codebase when operations are attempted on incompatible relation types.

The function uses a switch statement to map each RELKIND constant to a specific error message, ensuring that users receive clear feedback about why their operation failed. If an unrecognized relkind is passed, it raises an ERROR-level log message.

## Parameters / Member Variables
- : A character constant representing the type of relation (table, index, view, etc.) using PostgreSQL's RELKIND_* constants

## Dependencies
- Functions called/Symbols referenced:
  - [errdetail](errdetail.md) (for generating error detail messages)
  - elog (for logging unrecognized relkind errors)
  - RELKIND_RELATION (table constant)
  - RELKIND_INDEX (index constant) 
  - RELKIND_SEQUENCE (sequence constant)
  - RELKIND_TOASTVALUE (TOAST table constant)
  - RELKIND_VIEW (view constant)
  - RELKIND_MATVIEW (materialized view constant)
  - RELKIND_COMPOSITE_TYPE (composite type constant)
  - RELKIND_FOREIGN_TABLE (foreign table constant)
  - RELKIND_PARTITIONED_TABLE (partitioned table constant)
  - RELKIND_PARTITIONED_INDEX (partitioned index constant)

- Called from (representative examples):
  - [validate_relation_kind](../v/validate_relation_kind.md) (in sequence.c and table.c)
  - [check_publication_add_relation](../c/check_publication_add_relation.md) (in pg_publication.c)
  - [CommentObject](../C/CommentObject.md) (in comment.c)
  - [DefineIndex](../D/DefineIndex.md) (in indexcmds.c)
  - [RangeVarCallbackForLockTable](../R/RangeVarCallbackForLockTable.md) (in lockcmds.c)
  - [CreateStatistics](../C/CreateStatistics.md) (in statscmds.c)
  - [ATSimplePermissions](../A/ATSimplePermissions.md) (in tablecmds.c)
  - [transformMergeStmt](../t/transformMergeStmt.md) (in parse_merge.c)
  - [DefineQueryRewrite](../D/DefineQueryRewrite.md) (in rewriteDefine.c)

## Notes and Other Information
- This function is widely used throughout PostgreSQL's DDL (Data Definition Language) operations to provide consistent error messaging
- The function covers all major relation types in PostgreSQL, making it a comprehensive solution for relkind validation errors
- Returns an integer (likely for compatibility with errdetail() return conventions)
- Located in src/backend/catalog/pg_class.c, which is appropriate as it deals with relation classification
- The default case protects against programming errors by logging unrecognized relkind values
- This centralized approach helps maintain consistency in error messages across the entire PostgreSQL codebase

## Simplified Source

```c
int errdetail_relkind_not_supported(char relkind) {
    // Return appropriate error message based on relation type
    switch (relkind) {
        case RELKIND_RELATION:
            return errdetail("This operation is not supported for tables.");
        case RELKIND_INDEX:
            return errdetail("This operation is not supported for indexes.");
        case RELKIND_SEQUENCE:
            return errdetail("This operation is not supported for sequences.");
        case RELKIND_TOASTVALUE:
            return errdetail("This operation is not supported for TOAST tables.");
        case RELKIND_VIEW:
            return errdetail("This operation is not supported for views.");
        case RELKIND_MATVIEW:
            return errdetail("This operation is not supported for materialized views.");
        case RELKIND_COMPOSITE_TYPE:
            return errdetail("This operation is not supported for composite types.");
        case RELKIND_FOREIGN_TABLE:
            return errdetail("This operation is not supported for foreign tables.");
        case RELKIND_PARTITIONED_TABLE:
            return errdetail("This operation is not supported for partitioned tables.");
        case RELKIND_PARTITIONED_INDEX:
            return errdetail("This operation is not supported for partitioned indexes.");
        default:
            // Programming error - unknown relation kind
            elog(ERROR, "unrecognized relkind: '%c'", relkind);
            return 0;
    }
}
```