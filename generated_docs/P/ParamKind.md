# ParamKind

## Location
[src/include/nodes/primnodes.h:371-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L371-L372)

## Overview
ParamKind is an enumeration that specifies the different types of parameters used in PostgreSQL's query execution system, distinguishing between external parameters, internal executor parameters, and sublink-related parameters.

## Definition

```c
typedef struct Param
{
	Expr		xpr;
	ParamKind	paramkind;		/* kind of parameter. See above */
	int			paramid;		/* numeric ID for parameter */
	Oid			paramtype;		/* pg_type OID of parameter's datatype */
	/* typmod value, if known */
	int32		paramtypmod pg_node_attr(query_jumble_ignore);
	/* OID of collation, or InvalidOid if none */
	Oid			paramcollid pg_node_attr(query_jumble_ignore);
	/* token location, or -1 if unknown */
	ParseLoc	location;
} Param;
```
## Detailed Description
ParamKind defines four distinct parameter types used throughout PostgreSQL's query planning and execution phases:

- **PARAM_EXTERN**: External parameters supplied from outside the plan, typically from prepared statements or function calls. These are numbered from 1 to n and represent values passed into the query from the client or calling context.

- **PARAM_EXEC**: Internal executor parameters used for passing values between different parts of the execution tree, particularly for sub-queries and nestloop joins to their inner scans. For historical reasons, these are numbered from 0 and use a separate numbering scheme from PARAM_EXTERN.

- **PARAM_SUBLINK**: Parameters representing output columns of a SubLink node's sub-select. The column number is stored in the paramid field. These parameters are converted to PARAM_EXEC during the planning phase.

- **PARAM_MULTIEXPR**: Similar to PARAM_SUBLINK but specifically for MULTIEXPR SubLinks. The paramid field encodes both the SubLink's subLinkId (high-order 16 bits) and the column number (low-order 16 bits). Also converted to PARAM_EXEC during planning.

## Parameters / Member Variables
- : External parameter from outside the plan (numbered 1 to n)
- : Internal executor parameter for sub-queries and joins (numbered from 0)
- : SubLink output column parameter (converted to PARAM_EXEC during planning)
- : MULTIEXPR SubLink parameter with encoded subLinkId and column (converted to PARAM_EXEC during planning)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this enum)
- Called from (representative examples):
  - Param struct (uses ParamKind as paramkind field)

## Notes and Other Information
- PARAM_EXTERN and PARAM_EXEC use independent numbering schemes
- PARAM_SUBLINK and PARAM_MULTIEXPR are temporary parameter types that get converted to PARAM_EXEC during query planning
- The distinction between parameter kinds is crucial for proper parameter binding and execution in PostgreSQL's query engine
- PARAM_MULTIEXPR uses a packed encoding scheme in the paramid field to store both SubLink ID and column number