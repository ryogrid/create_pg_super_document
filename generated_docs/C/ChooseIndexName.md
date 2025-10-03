# ChooseIndexName

## Location
[src/backend/commands/indexcmds.c:2543-2597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2543-L2597)

## Overview
ChooseIndexName is a static function that selects an appropriate name for a PostgreSQL index based on the table name, column names, and index type (primary key, exclusion constraint, unique constraint, or regular index).

## Definition

```c
static char *
ChooseIndexName(const char *tabname, Oid namespaceId,
				const List *colnames, const List *exclusionOpNames,
				bool primary, bool isconstraint)
```
## Detailed Description
ChooseIndexName generates index names following PostgreSQL's naming conventions by delegating to ChooseRelationName with different suffixes based on the index type:
- Primary key indexes use the "pkey" suffix without column-specific naming
- Exclusion constraint indexes use the "excl" suffix with column names
- Regular constraint indexes use the "key" suffix with column names  
- Non-constraint indexes use the "idx" suffix with column names

The function uses ChooseIndexNameAddition to create a column-based name component for most index types, except primary keys which have standardized naming.

## Parameters / Member Variables
- `*tabname`: The name of the table for which the index is being created
- `namespaceId`: The OID of the namespace (schema) containing the table
- `*colnames`: List of column names that the index covers
- `*exclusionOpNames`: List of exclusion operator names (non-NIL for exclusion constraints)
- `primary`: Boolean flag indicating if this is a primary key index
- `isconstraint`: Boolean flag indicating if this index backs a constraint
## Dependencies
- Functions called/Symbols referenced:
  - [ChooseRelationName](ChooseRelationName.md) (called multiple times with different suffixes)
  - [ChooseIndexNameAddition](ChooseIndexNameAddition.md) (called to generate column-based name components)
- Called from:
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:821)

## Notes and Other Information
- This is a static function internal to indexcmds.c
- The comment notes that the argument list is "pretty ad-hoc", suggesting this interface evolved over time
- Primary key naming is standardized and doesn't include column names in the index name
- The function handles four distinct index naming scenarios with appropriate suffixes
- Uses different conflict resolution strategies (true/false for the last parameter to ChooseRelationName) depending on index type

## Simplified Source

```c
static char *ChooseIndexName(const char *tabname, Oid namespaceId,
                            const List *colnames, const List *exclusionOpNames,
                            bool primary, bool isconstraint) {
    char *indexname;

    if (primary) {
        // Primary key: use "pkey" suffix, no column names
        indexname = ChooseRelationName(tabname, NULL, "pkey", namespaceId, true);
    }
    else if (exclusionOpNames != NIL) {
        // Exclusion constraint: use "excl" suffix with column names
        indexname = ChooseRelationName(tabname, ChooseIndexNameAddition(colnames),
                                      "excl", namespaceId, true);
    }
    else if (isconstraint) {
        // Regular constraint: use "key" suffix with column names
        indexname = ChooseRelationName(tabname, ChooseIndexNameAddition(colnames),
                                      "key", namespaceId, true);
    }
    else {
        // Regular index: use "idx" suffix with column names
        indexname = ChooseRelationName(tabname, ChooseIndexNameAddition(colnames),
                                      "idx", namespaceId, false);
    }

    return indexname;
}
```