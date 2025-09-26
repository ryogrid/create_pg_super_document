# makeColumnDef

## Location
[src/backend/nodes/makefuncs.c:539-567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L539-L567)

## Overview
Constructs a ColumnDef node representing a simple column definition with specified type and collation, initializing all properties to basic default values.

## Definition
```c
ColumnDef *makeColumnDef(const char *colname, Oid typeOid, int32 typmod, Oid collOid)
```

## Detailed Description
The `makeColumnDef` function creates a ColumnDef node structure that represents a column definition in table creation and modification operations. It takes the essential column properties (name, type, and collation) and initializes a complete ColumnDef structure with sensible defaults for all other properties.

The function sets up a basic column definition that can be further customized by the caller. It uses OID-based type specification for efficiency and sets the column as local (not inherited) with no constraints or special properties initially.

## Parameters / Member Variables
- `colname`: The name of the column (string is duplicated for safe storage)
- `typeOid`: The object identifier of the column's data type
- `typmod`: The type modifier specifying additional type information
- `collOid`: The object identifier of the collation to use for this column

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for ColumnDef allocation)
  - pstrdup (for string duplication)
  - makeTypeNameFromOid (for type specification)
  - ColumnDef (struct type)
  - NIL (empty list constant)
- Called from (representative examples):
  - create_ctas_nodata
  - intorel_startup
  - DefineSequence
  - MergeAttributes
  - DefineVirtualRelation
  - transformTableLikeClause
  - transformOfType

## Notes and Other Information
- Sets inhcount to 0 (no inheritance count)
- Sets is_local to true (column is locally defined)
- Sets is_not_null to false (allows NULL values by default)
- Sets is_from_type to false (not derived from a type)
- Sets storage to 0 (default storage strategy)
- Sets both raw_default and cooked_default to NULL (no default value)
- Sets collClause to NULL (collation specified by OID)
- Sets constraints to NIL (no column constraints)
- Sets fdwoptions to NIL (no foreign data wrapper options)
- Sets location to -1 (unknown source location)
- Declared in src/include/nodes/makefuncs.h at line 76
- Commonly used in DDL operations, table creation, and schema manipulation