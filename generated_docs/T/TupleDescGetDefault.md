# TupleDescGetDefault

## Location
src/backend/access/common/tupdesc.c: 899 - 922

## Overview
Retrieves the default expression for a specified attribute in a tuple descriptor, returning NULL if no default is defined.

## Definition
```c
Node *TupleDescGetDefault(TupleDesc tupdesc, AttrNumber attnum)
```

## Detailed Description
TupleDescGetDefault searches through the constraint information stored in a tuple descriptor to find and return the default expression for a specified attribute. The function examines the defval array within the tuple descriptor's constraint structure, looking for an entry that matches the requested attribute number.

When a matching default value is found, the function converts the stored binary representation (adbin) back into a Node tree structure using stringToNode. This allows the default expression to be used in query planning and execution contexts where Node trees are expected.

The function returns NULL if either the tuple descriptor has no constraints defined, or if no default value is specified for the requested attribute.

## Parameters / Member Variables
- `tupdesc`: The tuple descriptor to search for default values
- `attnum`: The attribute number (1-based) for which to retrieve the default expression

## Dependencies
- Functions called/Symbols referenced:
  - stringToNode (converts string representation back to Node tree)
  - AttrDefault (structure type for storing attribute defaults)
- Called from (representative examples):
  - MergeAttributes (table inheritance processing)
  - expandTableLikeClause (CREATE TABLE LIKE clause handling)
  - build_column_default (rewrite rule processing)

## Notes and Other Information
- Returns NULL if the tuple descriptor has no constraints or no default for the specified attribute
- The returned Node tree is a newly allocated copy, not a reference to the original
- Default expressions are stored in binary format (adbin) within the AttrDefault structure
- The function performs a linear search through the defval array - this is acceptable since the number of defaults is typically small
- Used primarily during DDL operations and query rewriting where default expressions need to be processed
- The attribute number is 1-based following PostgreSQL conventions