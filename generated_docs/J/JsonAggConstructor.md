# JsonAggConstructor

## Location
[src/include/nodes/parsenodes.h:1962-1970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1962-L1970)

## Overview
JsonAggConstructor represents the common fields for the untransformed (parse tree) representation of JSON aggregate functions JSON_ARRAYAGG() and JSON_OBJECTAGG().

## Definition

```c
typedef struct JsonAggConstructor
{
	NodeTag		type;
	JsonOutput *output;			/* RETURNING clause, if any */
	Node	   *agg_filter;		/* FILTER clause, if any */
	List	   *agg_order;		/* ORDER BY clause, if any */
	struct WindowDef *over;		/* OVER clause, if any */
	ParseLoc	location;		/* token location, or -1 if unknown */
} JsonAggConstructor;
```
## Detailed Description
JsonAggConstructor serves as a base structure containing common elements shared by JSON aggregate functions during the parsing phase. It captures the various optional clauses that can be applied to JSON aggregation operations, including output format specification, filtering conditions, ordering requirements, and window function specifications. This structure is used before transformation into execution-ready forms.

## Parameters / Member Variables
- : NodeTag identifying this as a JsonAggConstructor node
- : Pointer to JsonOutput structure specifying the RETURNING clause format, if present
- : Node representing the FILTER clause condition for conditional aggregation, if any
- : List of ordering expressions for the ORDER BY clause, if specified
- : Pointer to WindowDef structure for OVER clause in window function context, if present  
- : Parse location information for error reporting, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - [JsonOutput](JsonOutput.md)
  - [WindowDef](../W/WindowDef.md)
  - ParseLoc
- Called from (representative examples):
  - [exprLocation](../e/exprLocation.md)
  - [transformJsonAggConstructor](../t/transformJsonAggConstructor.md)
  - [transformJsonArrayQueryConstructor](../t/transformJsonArrayQueryConstructor.md)
  - [JsonObjectAgg](JsonObjectAgg.md)
  - [JsonArrayAgg](JsonArrayAgg.md)

## Notes and Other Information
- This structure is used as a common base for both JSON_ARRAYAGG and JSON_OBJECTAGG parsing
- The structure supports all standard aggregate function features including filtering, ordering, and windowing
- Location information is preserved for accurate error reporting during parsing and transformation phases
- The over field enables these JSON aggregates to function as window functions