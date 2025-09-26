# JsonOutput

## Location
[src/include/nodes/parsenodes.h:1751-1756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1751-L1756)

## Overview
A structure representing the JSON output clause (RETURNING type [FORMAT format]) used in SQL/JSON expressions to specify the return type and format for JSON operations.

## Definition

```c
typedef struct JsonOutput
{
	NodeTag		type;
	TypeName   *typeName;		/* RETURNING type name, if specified */
	JsonReturning *returning;	/* RETURNING FORMAT clause and type Oids */
} JsonOutput;
```
## Detailed Description
JsonOutput is a parse tree node that represents the output specification for JSON functions and expressions in PostgreSQL's SQL/JSON support. It encapsulates the RETURNING clause which allows users to specify the data type and format for JSON operation results. This structure provides flexibility in how JSON data is returned, allowing conversion to various PostgreSQL data types and controlling the output format. The structure serves as an intermediate representation during parsing that is later processed to determine the actual return type and format handling for JSON operations.

## Parameters / Member Variables
- `type`: Standard NodeTag for PostgreSQL node identification
- `*typeName`: Pointer to the specified return type name in the RETURNING clause (NULL if not specified)
- `*returning`: Pointer to JsonReturning structure containing FORMAT clause details and resolved type OIDs
## Dependencies
- Functions called/Symbols referenced:
  - [TypeName](../T/TypeName.md)
  - [JsonReturning](JsonReturning.md)
- Called from (representative examples):
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)
  - [transformJsonOutput](../t/transformJsonOutput.md)
  - [transformJsonConstructorOutput](../t/transformJsonConstructorOutput.md)
  - [transformJsonReturning](../t/transformJsonReturning.md)
  - [transformJsonParseExpr](../t/transformJsonParseExpr.md)
  - [transformJsonScalarExpr](../t/transformJsonScalarExpr.md)
  - [transformJsonTableColumn](../t/transformJsonTableColumn.md)

## Notes and Other Information
- Part of PostgreSQL's SQL/JSON support framework introduced for JSON processing capabilities
- Used across various JSON expression types including JsonFuncExpr, JsonParseExpr, JsonScalarExpr, JsonSerializeExpr, and JSON constructor expressions
- The typeName can be NULL if no specific RETURNING type is specified, allowing for default type inference
- Works in conjunction with JsonReturning to provide comprehensive output control for JSON operations
- Located in src/include/nodes/parsenodes.h at lines 1751-1756