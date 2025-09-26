# JsonReturning

## Location
[src/include/nodes/primnodes.h:1660-1666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1660-L1666)

## Overview
JsonReturning represents the transformed representation of a JSON RETURNING clause, used to specify the output format and type information for JSON operations.

## Definition

```c
typedef struct JsonReturning
{
	NodeTag		type;
	JsonFormat *format;			/* output JSON format */
	Oid			typid;			/* target type Oid */
	int32		typmod;			/* target type modifier */
} JsonReturning;
```
## Detailed Description
JsonReturning is a node structure that encapsulates the specifications for how JSON results should be returned from JSON operations. It serves as the internal representation of SQL JSON RETURNING clauses, containing information about the desired output format and the target data type for the result.

This structure is created during the parsing and transformation phase when processing JSON expressions that include RETURNING clauses, and is used throughout the execution pipeline to ensure proper formatting and type conversion of JSON results.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `*format`: Pointer to JsonFormat structure specifying the output JSON format details
- `typid`: OID of the target PostgreSQL data type for the returned value
- `typmod`: Type modifier providing additional type-specific information (e.g., precision, length)
## Dependencies
- Functions called/Symbols referenced:
  - [JsonFormat](JsonFormat.md)

- Called from (representative examples):
  - [transformJsonOutput](../t/transformJsonOutput.md)
  - [transformJsonConstructorOutput](../t/transformJsonConstructorOutput.md)
  - [coerceJsonFuncExpr](../c/coerceJsonFuncExpr.md)
  - [makeJsonConstructorExpr](../m/makeJsonConstructorExpr.md)
  - [transformJsonObjectConstructor](../t/transformJsonObjectConstructor.md)
  - [transformJsonAggConstructor](../t/transformJsonAggConstructor.md)
  - [transformJsonObjectAgg](../t/transformJsonObjectAgg.md)
  - [transformJsonArrayAgg](../t/transformJsonArrayAgg.md)
  - [transformJsonArrayConstructor](../t/transformJsonArrayConstructor.md)
  - [transformJsonIsPredicate](../t/transformJsonIsPredicate.md)
  - [transformJsonReturning](../t/transformJsonReturning.md)
  - [transformJsonParseExpr](../t/transformJsonParseExpr.md)
  - [transformJsonScalarExpr](../t/transformJsonScalarExpr.md)
  - [transformJsonSerializeExpr](../t/transformJsonSerializeExpr.md)
  - [transformJsonFuncExpr](../t/transformJsonFuncExpr.md)
  - [transformJsonBehavior](../t/transformJsonBehavior.md)

## Notes and Other Information
- This structure is part of the PostgreSQL node system and follows standard node conventions
- It's primarily used in JSON expression processing and execution contexts
- The structure bridges the gap between SQL-level JSON RETURNING syntax and internal execution requirements
- Located in src/include/nodes/primnodes.h:1660-1666