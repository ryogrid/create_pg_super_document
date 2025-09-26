# A_Const

## Location
[src/include/nodes/parsenodes.h:357-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L357-L365)

## Overview
A_Const represents a constant value in PostgreSQL's parse tree, used to store literal values encountered during SQL parsing such as numbers, strings, booleans, and NULL constants.

## Definition

```c
typedef struct A_Const
{
	pg_node_attr(custom_copy_equal, custom_read_write, custom_query_jumble)

	NodeTag		type;
	union ValUnion val;
	bool		isnull;			/* SQL NULL constant */
	ParseLoc	location;		/* token location, or -1 if unknown */
} A_Const;
```
## Detailed Description
A_Const is a fundamental parse tree node that encapsulates constant values found in SQL statements. It serves as a container for various types of literal values including integers, floats, strings, booleans, and NULL values. The structure is designed to preserve both the value and its location in the original SQL text for error reporting and debugging purposes. The node includes custom attributes for copying, equality checking, and query jumbling operations.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an A_Const node
- `val`: Union containing the actual constant value (ValUnion type)
- `isnull`: Boolean flag indicating whether this represents a SQL NULL constant
- `location`: ParseLoc storing the token's position in the source SQL, or -1 if location is unknown
## Dependencies
- Functions called/Symbols referenced:
  - ValUnion
  - ParseLoc
- Called from (representative examples):
  - [_copyA_Const](../c/_copyA_Const.md)
  - [_equalA_Const](../e/_equalA_Const.md)
  - [makeStringConst](../m/makeStringConst.md)
  - [exprLocation](../e/exprLocation.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [make_const](../m/make_const.md)

## Notes and Other Information
- [A_Const](A_Const.md) nodes are created during parsing and are typically transformed into Const nodes during the analysis phase
- The pg_node_attr annotation indicates special handling for copy, equality, read/write, and query jumbling operations
- Location information is crucial for providing accurate error messages when parsing fails
- The ValUnion allows storage of different data types in a memory-efficient manner