# oidparse

## Location
[src/backend/utils/adt/oid.c:235-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L235-L257)

## Overview
Utility function that extracts an OID value from PostgreSQL's parser node structures (Integer or Float constants).

## Definition

```c
Oid
oidparse(Node *node)
```
## Detailed Description
The  function is a utility function used during SQL parsing and compilation to extract OID values from constant nodes in the parse tree. It handles both Integer and Float constant nodes, which can occur when OID values are specified in SQL commands.

The function handles two cases: T_Integer nodes are processed directly using , while T_Float nodes (which represent values too large for int4) are processed using  to parse the string representation. This dual handling ensures that OID values can be correctly parsed regardless of how the lexer categorized the numeric constant.

## Parameters / Member Variables
- : Pointer to a PostgreSQL parser Node structure that should contain an Integer or Float constant representing an OID value

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (macro to get node type)
  - intVal (macro to extract integer value)
  - [uint32in_subr](../u/uint32in_subr.md) (OID parsing function)
  - castNode (type-safe node casting macro)
  - elog (error logging)
- Called from (representative examples):
  - [objectNamesToOids](objectNamesToOids.md) (src/backend/catalog/aclchk.c:733)
  - [get_object_address](../g/get_object_address.md) (src/backend/catalog/objectaddress.c:1047)

## Notes and Other Information
- Handles the lexer's categorization of large numeric values as Float constants
- Uses uint32in_subr for Float nodes to properly validate OID range
- Returns InvalidOid as a fallback (though this should never be reached due to the error case)
- Part of PostgreSQL's SQL parsing infrastructure for handling OID literals
- Essential for commands that reference database objects by OID
- Provides proper error handling for unexpected node types with descriptive error messages