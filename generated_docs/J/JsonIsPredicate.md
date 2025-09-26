# JsonIsPredicate

## Location
src/include/nodes/primnodes.h: 1732 - 1740

## Overview
JsonIsPredicate represents the IS JSON predicate used to test whether an expression contains valid JSON data, with options for format specification and item type checking.

## Definition
```c
typedef struct JsonIsPredicate
{
	NodeTag		type;
	Node	   *expr;			/* subject expression */
	JsonFormat *format;			/* FORMAT clause, if specified */
	JsonValueType item_type;	/* JSON item type */
	bool		unique_keys;	/* check key uniqueness? */
	ParseLoc	location;		/* token location, or -1 if unknown */
} JsonIsPredicate;
```

## Detailed Description
JsonIsPredicate implements the SQL/JSON IS JSON predicate functionality, which allows testing whether a given expression contains valid JSON data. This predicate supports various options to refine the validation criteria, including specific JSON item type checking (e.g., IS JSON OBJECT, IS JSON ARRAY) and key uniqueness validation for JSON objects.

The structure can optionally include a FORMAT clause to specify the expected input format of the data being tested. This is particularly useful when the source data might be in different formats that need to be interpreted as JSON.

The predicate is commonly used in conditional expressions, WHERE clauses, and CHECK constraints to ensure data integrity and proper JSON structure validation before processing.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `expr`: Pointer to the expression being tested for JSON validity
- `format`: Pointer to JsonFormat structure specifying input format, if FORMAT clause was specified
- `item_type`: JsonValueType enum specifying the specific JSON type to validate (JSON, JSON OBJECT, JSON ARRAY, etc.)
- `unique_keys`: Boolean flag indicating whether to enforce key uniqueness in JSON objects
- `location`: Parse location for error reporting and debugging, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - JsonFormat
  - JsonValueType
  - ParseLoc
  - Node

- Called from (representative examples):
  - ExecInitExprRec
  - ExecEvalJsonIsPredicate
  - makeJsonIsPredicate
  - transformJsonIsPredicate
  - transformExprRecurse
  - get_rule_expr
  - exprLocation
  - raw_expression_tree_walker_impl

## Notes and Other Information
- Supports various JSON item type validations beyond basic JSON validity
- The unique_keys option provides additional validation for JSON object structures
- Can be used in both runtime validation and compile-time constraint checking
- Part of PostgreSQL's comprehensive SQL/JSON standard compliance
- Located in src/include/nodes/primnodes.h:1732-1740
- Essential for applications requiring strict JSON data validation