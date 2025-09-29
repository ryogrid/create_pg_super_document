# index_check_primary_key

## Location
[src/backend/catalog/index.c:201-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L201-L279)

## Overview
Validates that a relation can have a primary key index created by checking for existing primary keys, ensuring columns are simple references (not expressions), and verifying all columns are marked NOT NULL.

## Definition

```c
void
index_check_primary_key(Relation heapRel,
						const IndexInfo *indexInfo,
						bool is_alter_table,
						const IndexStmt *stmt)
```
## Detailed Description
This function performs essential validation checks before creating a PRIMARY KEY index. It was originally part of DefineIndex() but was extracted to support ALTER TABLE ADD PRIMARY KEY USING INDEX operations. The function enforces several PostgreSQL constraints: (1) prevents creation of multiple primary keys on a table, (2) ensures primary key indexes don't use NULLS NOT DISTINCT, (3) validates that all indexed columns are simple column references rather than expressions, and (4) confirms all primary key columns are marked NOT NULL. The function expects the parser to have already inserted any required ALTER TABLE SET NOT NULL operations before attempting to create the primary key.

## Parameters / Member Variables
- : Relation pointer to the table where the primary key will be created (caller must hold at least ShareLock)
- : IndexInfo structure containing details about the index being created, including column information
- : Boolean flag indicating whether this is part of an ALTER TABLE operation
- : IndexStmt structure containing the index statement details (may be NULL in some contexts)

## Dependencies
- Functions called/Symbols referenced:
  - [relationHasPrimaryKey](../r/relationHasPrimaryKey.md): Checks if the relation already has a primary key
  - [IndexInfo](../I/IndexInfo.md): Structure containing index metadata
  - [IndexStmt](../I/IndexStmt.md): Structure representing index creation statement
  - [SearchSysCache2](../S/SearchSysCache2.md): Searches system cache for attribute information
  - [Int16GetDatum](../I/Int16GetDatum.md): Converts integer to PostgreSQL Datum format
  - RelationGetRelationName: Gets relation name for error messages
  - RelationGetRelid: Gets relation OID
  - HeapTupleIsValid: Validates heap tuple
  - Form_pg_attribute: PostgreSQL system catalog structure for attribute information
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md): During index creation operations
  - [ATExecAddIndexConstraint](../A/ATExecAddIndexConstraint.md): During ALTER TABLE operations that add constraints

## Notes and Other Information
- The function performs different checks based on whether it's an ALTER TABLE operation or partition table creation
- System attributes (negative attnum) are automatically considered NOT NULL and skip validation
- NULLS NOT DISTINCT indexes cannot be used for primary keys due to uniqueness requirements
- Error handling provides detailed messages for different constraint violations
- The function assumes proper locking (ShareLock minimum) for reliable NOT NULL checking
- Historical behavior of automatically setting columns to NOT NULL was removed to avoid operation ordering issues in complex ALTER TABLE commands

## Simplified Source

```c
void
index_check_primary_key(Relation heapRel,
                        const IndexInfo *indexInfo,
                        bool is_alter_table,
                        const IndexStmt *stmt)
{
    // Check for existing primary key in ALTER TABLE or partition scenarios
    if ((is_alter_table || heapRel->rd_rel->relispartition) &&
        relationHasPrimaryKey(heapRel))
    {
        ereport(ERROR, "multiple primary keys not allowed");
    }

    // Primary keys cannot use NULLS NOT DISTINCT indexes
    if (indexInfo->ii_NullsNotDistinct)
    {
        ereport(ERROR, "primary keys cannot use NULLS NOT DISTINCT indexes");
    }

    // Validate each indexed column
    for (int i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
    {
        AttrNumber attnum = indexInfo->ii_IndexAttrNumbers[i];

        // Primary keys cannot be expressions
        if (attnum == 0)
            ereport(ERROR, "primary keys cannot be expressions");

        // Skip system attributes (they're always NOT NULL)
        if (attnum < 0)
            continue;

        // Look up attribute information in system cache
        HeapTuple atttuple = SearchSysCache2(ATTNUM,
                                           ObjectIdGetDatum(RelationGetRelid(heapRel)),
                                           Int16GetDatum(attnum));

        if (!HeapTupleIsValid(atttuple))
            elog(ERROR, "cache lookup failed for attribute %d", attnum);

        Form_pg_attribute attform = (Form_pg_attribute) GETSTRUCT(atttuple);

        // Ensure column is marked NOT NULL
        if (!attform->attnotnull)
            ereport(ERROR, "primary key column \"%s\" is not marked NOT NULL",
                    NameStr(attform->attname));

        ReleaseSysCache(atttuple);
    }
}
```