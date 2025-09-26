# MergeSupportFunc

## Location
[src/include/nodes/primnodes.h:628-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L628-L637)

## Overview
MergeSupportFunc represents a merge support function expression that can only appear in the RETURNING list of a MERGE command, providing information about the currently executing merge action.

## Definition

```c
typedef struct MergeSupportFunc
{
	Expr		xpr;
	/* type Oid of result */
	Oid			msftype;
	/* OID of collation, or InvalidOid if none */
	Oid			msfcollid;
	/* token location, or -1 if unknown */
	ParseLoc	location;
} MergeSupportFunc;
```
## Detailed Description
MergeSupportFunc is a specialized expression node designed specifically for PostgreSQL's MERGE statement. It represents support functions that provide metadata about the merge operation currently being executed. This structure can only be used within the RETURNING clause of MERGE commands and serves to expose internal merge operation state to the user.

Currently, the primary supported function is MERGE_ACTION(), which returns a string indicating which DML action ("INSERT", "UPDATE", or "DELETE") was performed for the current row during merge execution. This allows applications to determine what specific action was taken for each row processed by the MERGE statement.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : Data type OID of the function's return value (typically text for MERGE_ACTION())
- : Collation OID for the result, or InvalidOid if no specific collation is needed
- : Parse location in the original query text for error reporting and debugging purposes

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (for location tracking)
  - Expr (base expression structure)
  - Oid (for type and collation references)
  
- Called from (representative examples):
  - transformMergeSupportFunc (parser transformation of merge support function calls)
  - exprType, exprCollation, exprLocation (expression node utility functions)
  - replace_correlation_vars_mutator (query rewriting for correlated variables)
  - replace_outer_merge_support (parameter assignment optimization)

## Notes and Other Information
- Restricted to use only in RETURNING clauses of MERGE statements - cannot be used in other contexts
- Currently supports only MERGE_ACTION() function, but the structure is designed to accommodate future merge support functions
- Essential for applications that need to track which specific actions were performed during bulk merge operations
- The location field enables accurate error reporting when merge support functions are used incorrectly
- Part of PostgreSQL's MERGE statement implementation, which provides UPSERT-like functionality combining INSERT, UPDATE, and DELETE operations
- Enables observability into merge operation results, allowing applications to implement complex business logic based on which actions were actually performed