# btproperty

## Location
src/backend/access/nbtree/nbtutils.c: 4586 - 4608

## Overview
The btproperty function checks boolean properties of B-tree indexes, providing efficient property queries without requiring the index relation to be opened.

## Definition
```c
bool btproperty(Oid index_oid, int attno, IndexAMProperty prop, const char *propname, bool *res, bool *isnull)
```

## Detailed Description
This function is an optional access method property checker for B-tree indexes. It handles specific property queries about B-tree indexes and their columns. Currently, it primarily handles the AMPROP_RETURNABLE property, which indicates whether the index can return the actual indexed values (as opposed to just providing ordering information).

The function provides an optimization by handling AMPROP_RETURNABLE queries directly without requiring the index relation to be opened, which would be necessary if the generic btcanreturn function were called instead.

For B-tree indexes, columns are always returnable since B-tree stores the actual key values, making index-only scans possible when all required columns are covered by the index.

## Parameters / Member Variables
- `index_oid`: Object identifier of the index being queried
- `attno`: Attribute number (column number) being queried, or 0 for index-wide properties
- `prop`: The IndexAMProperty being queried
- `propname`: String name of the property (for debugging/error reporting)
- `res`: Pointer to store the boolean result
- `isnull`: Pointer to store whether the result is null

## Dependencies
- Functions called/Symbols referenced:
  - IndexAMProperty (enum type)
  - AMPROP_RETURNABLE (constant)
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
This function is part of PostgreSQL's access method interface and is called during query planning to determine index capabilities. The AMPROP_RETURNABLE property is crucial for enabling index-only scans, which can significantly improve query performance by avoiding heap access when all required data is available in the index.