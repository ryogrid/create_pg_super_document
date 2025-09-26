# JsonTablePathSpec

## Location
[src/include/nodes/parsenodes.h:1807-1815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1807-L1815)

## Overview
JsonTablePathSpec represents an untransformed specification of a JSON path expression with an optional name, used in JSON table functionality to define path expressions for accessing JSON data.

## Definition

```c
typedef struct JsonTablePathSpec
{
	NodeTag		type;

	Node	   *string;
	char	   *name;
	ParseLoc	name_location;
	ParseLoc	location;		/* location of 'string' */
} JsonTablePathSpec;
```
## Detailed Description
JsonTablePathSpec is a parse node structure that holds information about JSON path specifications used in JSON table operations. It contains the raw path expression string along with optional naming information and location tracking for parser error reporting. This structure is part of the untransformed parse tree and gets processed during the transformation phase of query planning.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a JsonTablePathSpec node
- `*string`: Node representing the JSON path expression string
- `*name`: Optional name identifier for the path specification
- `name_location`: ParseLoc tracking the location of the name in the source query
- `location`: ParseLoc tracking the location of the path string in the source query
## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (for location tracking)
  - NodeTag (inherited node type system)
- Called from (representative examples):
  - [makeJsonTablePathSpec](../m/makeJsonTablePathSpec.md) (constructor function)
  - [transformJsonTable](../t/transformJsonTable.md) (transformation processing)
  - [transformJsonTableColumns](../t/transformJsonTableColumns.md) (column processing)
  - [makeJsonTablePathScan](../m/makeJsonTablePathScan.md) (scan node creation)

## Notes and Other Information
- This structure is part of the JSON table functionality introduced for SQL/JSON support
- Location information is crucial for providing accurate error messages during parsing
- The string field contains the actual JSON path expression that will be evaluated
- Used as building blocks for more complex JSON table column specifications