# _attrDefInfo

## Location
[src/bin/pg_dump/pg_dump.h:388-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L388-L394)

## Overview
The  structure stores information about DEFAULT expressions for table columns in pg_dump, representing individual column default value definitions.

## Definition


## Detailed Description
The  structure encapsulates information about DEFAULT expressions defined on table columns. It serves as a container for storing the textual representation of default value expressions along with metadata that links the default to its corresponding table and column. This structure is essential for pg_dump to properly reconstruct column defaults during database restoration.

## Parameters / Member Variables
- : Base dumpable object information; notably, the  field contains the name of the table that owns this default expression
- : Pointer to the TableInfo structure representing the table that contains the column with this default expression
- : The attribute number (column number) within the table for which this default expression is defined
- : The decompiled DEFAULT expression as a string, representing the actual default value or expression that will be applied to the column

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [TableInfo](../T/TableInfo.md)
- Called from (representative examples):
  - [_tableInfo](../t/_tableInfo.md) (referenced in the attrdefs field)

## Notes and Other Information
This structure is part of the broader schema metadata management system in pg_dump. The  field contains the human-readable SQL expression that represents the default value, which may range from simple literals to complex expressions involving functions or other columns. The structure maintains referential integrity with its parent table through the  pointer, ensuring that default expressions are properly associated with their corresponding tables and columns during the dump and restore process.