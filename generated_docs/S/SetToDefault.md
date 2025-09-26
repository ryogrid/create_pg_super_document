# SetToDefault

## Location
[src/include/nodes/primnodes.h:2068-2079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2068-L2079)

## Overview
A placeholder node representing a DEFAULT marker in INSERT or UPDATE commands that must be replaced with the actual column default expression during query rewriting.

## Definition

```c
typedef struct SetToDefault
{
	Expr		xpr;
	/* type for substituted value */
	Oid			typeId;
	/* typemod for substituted value */
	int32		typeMod pg_node_attr(query_jumble_ignore);
	/* collation for the substituted value */
	Oid			collation pg_node_attr(query_jumble_ignore);
	/* token location, or -1 if unknown */
	ParseLoc	location;
} SetToDefault;
```
## Detailed Description
SetToDefault is a specialized expression node that serves as a placeholder for DEFAULT markers in INSERT and UPDATE statements. It is not an executable expression but rather a temporary construct used during parsing and rewriting phases. When PostgreSQL encounters a DEFAULT keyword in a query, it creates a SetToDefault node that contains metadata about the expected default value. During the rewriting phase, these placeholder nodes are replaced with the actual default expressions from the table definition.

The structure inherits from Expr, making it compatible with PostgreSQL's expression tree framework, but it requires special handling since it cannot be directly executed. The node stores type information (typeId, typeMod, collation) that will be needed when the actual default value is substituted.

## Parameters / Member Variables
- `xpr`: Base expression node structure (inherited from Expr)
- `typeId`: OID of the data type for the value that will replace this placeholder
- `pg_node_attr(query_jumble_ignore)`: Type modifier for the substituted value (ignored in query jumbling)
- `pg_node_attr(query_jumble_ignore)`: Collation OID for the substituted value (ignored in query jumbling)
- `location`: Parse location in the original query text, or -1 if location is unknown
## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (for location tracking)
- Called from (representative examples):
  - [exprType](../e/exprType.md) (src/backend/nodes/nodeFuncs.c:266)
  - [transformTargetEntry](../t/transformTargetEntry.md) (src/backend/parser/parse_target.c:90)
  - [rewriteTargetListIU](../r/rewriteTargetListIU.md) (src/backend/rewrite/rewriteHandler.c:856)
  - [searchForDefault](../s/searchForDefault.md) (src/backend/rewrite/rewriteHandler.c:1302)

## Notes and Other Information
- This node is created during parsing when DEFAULT keywords are encountered
- It must be eliminated during the rewriting phase - any remaining SetToDefault nodes after rewriting indicate an error
- The pg_node_attr(query_jumble_ignore) annotations on typeMod and collation indicate these fields should be ignored when generating query fingerprints for plan caching
- Used primarily in INSERT and UPDATE statement processing
- The actual default value substitution logic is handled by the rewriter, particularly in functions like rewriteTargetListIU