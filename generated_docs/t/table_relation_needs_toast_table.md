# table_relation_needs_toast_table

## Location
[src/include/access/tableam.h:1878-1887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1878-L1887)

## Overview
Determines whether a table relation requires a TOAST (The Oversized-Attribute Storage Technique) table for storing large values.

## Definition

```c
static inline bool
table_relation_needs_toast_table(Relation rel)
```
## Detailed Description
This function provides a table access method interface for determining if a given relation needs a TOAST table. TOAST tables are auxiliary tables used in PostgreSQL to store large attribute values that exceed the maximum tuple size or page size limits. The function delegates to the underlying table access method's relation_needs_toast_table function, allowing different storage engines to implement their own logic for determining TOAST table requirements.

Different table access methods may have varying requirements for when TOAST tables are needed based on their storage characteristics, tuple size limits, and compression capabilities.

## Parameters / Member Variables
- : A Relation pointer representing the table relation to check for TOAST table requirements

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_needs_toast_table (table access method function pointer)
- Called from (representative examples):
  - [needs_toast_table](../n/needs_toast_table.md) (in src/backend/catalog/toasting.c:426)

## Notes and Other Information
- This is an inline function defined in the tableam header file for efficient access
- Part of the table access method abstraction layer that allows different storage engines
- TOAST tables are essential for PostgreSQL's ability to handle large data values that exceed page size limits
- The decision logic is delegated to the specific table access method implementation
- Located in src/include/access/tableam.h:1878-1887