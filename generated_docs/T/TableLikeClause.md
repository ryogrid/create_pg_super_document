# TableLikeClause

## Location
src/include/nodes/parsenodes.h: 751 - 757

## Overview
TableLikeClause represents a LIKE clause in CREATE TABLE statements, allowing a new table to inherit structure and optionally other properties from an existing table.

## Definition
```c
typedef struct TableLikeClause
{
    NodeTag     type;
    RangeVar   *relation;
    bits32      options;        /* OR of TableLikeOption flags */
    Oid         relationOid;    /* If table has been looked up, its OID */
} TableLikeClause;
```

## Detailed Description
TableLikeClause implements the SQL standard's LIKE clause functionality in CREATE TABLE statements, enabling table structure inheritance. The clause specifies which existing table to copy from and which aspects should be inherited through option flags. The structure supports both the syntactic representation (with RangeVar) and the resolved form (with relationOid after lookup). Various TableLikeOption flags control whether to inherit constraints, defaults, indexes, and other table properties.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a TableLikeClause node
- `relation`: RangeVar specifying the source table to copy structure from
- `options`: Bitfield of TableLikeOption flags controlling what properties to inherit
- `relationOid`: OID of the source table after resolution, used for efficient access

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](../R/RangeVar.md)
  - bits32
- Called from (representative examples):
  - [transformCreateStmt](../t/transformCreateStmt.md)
  - [transformTableLikeClause](../t/transformTableLikeClause.md)
  - [expandTableLikeClause](../e/expandTableLikeClause.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:751-757
- Part of CREATE TABLE statement processing for inheritance functionality
- Option flags determine which aspects are inherited (columns, constraints, defaults, etc.)
- relationOid is populated during semantic analysis for efficient table lookup
- Supports the SQL standard LIKE clause syntax for table definition inheritance