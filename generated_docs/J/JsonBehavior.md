# JsonBehavior

## Location
[src/include/nodes/primnodes.h:1786-1794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1786-L1794)

## Overview
JsonBehavior specifies the ON ERROR and ON EMPTY behavior handling for SQL/JSON query functions, defining what expressions to evaluate when error or empty conditions occur.

## Definition
```c
typedef struct JsonBehavior
{
	NodeTag		type;
	
	JsonBehaviorType btype;
	Node	   *expr;
	bool		coerce;
	ParseLoc	location;		/* token location, or -1 if unknown */
} JsonBehavior;
```

## Detailed Description
JsonBehavior defines the behavior specifications for handling exceptional conditions (errors and empty results) in SQL/JSON query functions. This structure is used to implement the ON ERROR and ON EMPTY clauses that are part of the SQL/JSON standard, allowing users to specify custom responses to these conditions.

When a JSON query function encounters an error condition (such as invalid JSON or type conversion failures) or an empty result condition (such as no matching elements), the JsonBehavior structure determines what action to take. This can range from returning specific default values to raising exceptions.

The structure includes an expression that will be evaluated when the specified behavior condition occurs, along with information about whether type coercion is needed to match the expected return type of the enclosing JsonExpr.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `btype`: JsonBehaviorType enum specifying the type of behavior (ON ERROR, ON EMPTY, etc.)
- `expr`: Pointer to the expression to evaluate when this behavior condition occurs
- `coerce`: Boolean flag indicating whether the expr needs type coercion to match JsonExpr.returning type
- `location`: Parse location for error reporting and debugging, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - JsonBehaviorType
  - ParseLoc
  - [Node](../N/Node.md)

- Called from (representative examples):
  - [GetJsonBehaviorValueString](../G/GetJsonBehaviorValueString.md)
  - [makeJsonValueExpr](../m/makeJsonValueExpr.md)
  - [makeJsonBehavior](../m/makeJsonBehavior.md)
  - [transformJsonBehavior](../t/transformJsonBehavior.md)
  - [ValidJsonBehaviorDefaultExpr](../V/ValidJsonBehaviorDefaultExpr.md)
  - [get_json_behavior](../g/get_json_behavior.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [exprCollation](../e/exprCollation.md)
  - [exprSetCollation](../e/exprSetCollation.md)
  - [exprLocation](../e/exprLocation.md)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)

## Notes and Other Information
- Essential component for implementing SQL/JSON standard compliance in error handling
- The coerce flag ensures proper type matching with the containing JsonExpr's return type
- Supports both ERROR and EMPTY behavior specifications as defined in the SQL/JSON standard
- Used extensively in JSON table functions and JSON query expressions
- Located in src/include/nodes/primnodes.h:1786-1794
- Provides flexible exception handling mechanisms for robust JSON data processing