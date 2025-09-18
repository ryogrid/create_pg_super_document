# _readBoolExpr

## Location
src/backend/nodes/readfuncs.c: 281 - 303

## Overview
A static function that deserializes BoolExpr nodes from their textual representation, handling boolean expression operations (AND, OR, NOT) used in PostgreSQL query processing and expression evaluation.

## Definition
```c
static BoolExpr *_readBoolExpr(void)
```

## Detailed Description
The `_readBoolExpr` function deserializes BoolExpr nodes, which represent boolean expressions in PostgreSQL's expression trees. These nodes are fundamental for logical operations in WHERE clauses, CHECK constraints, and other boolean contexts within queries.

The function employs a "do-it-yourself enum representation" approach, manually parsing string tokens to determine the boolean operation type:

1. **Operation parsing**: Reads the `:boolop` field marker, then parses the operation string ("and", "or", "not")
2. **Enum conversion**: Maps string representations to their corresponding enum values (`AND_EXPR`, `OR_EXPR`, `NOT_EXPR`)
3. **Arguments processing**: Deserializes the list of argument expressions using `READ_NODE_FIELD(args)`
4. **Location handling**: Preserves source location information for error reporting and debugging

The function provides comprehensive error handling for unrecognized boolean operations, ensuring robust parsing of expression trees during node deserialization.

## Parameters / Member Variables
- No parameters (uses standard node reading context via `READ_LOCALS(BoolExpr)`)
- Returns: `BoolExpr *` - pointer to the deserialized BoolExpr node with populated `boolop`, `args`, and `location` fields

## Dependencies
- Functions called/Symbols referenced:
  - `READ_LOCALS` (macro for local node reading setup)
  - [pg_strtok](../p/pg_strtok.md) (tokenization function)
  - `strncmp` (string comparison)
  - `AND_EXPR`, `OR_EXPR`, `NOT_EXPR` (enum constants)
  - `READ_NODE_FIELD` (macro for reading node list fields)
  - `READ_LOCATION_FIELD` (macro for reading location information)
  - `READ_DONE` (macro for node reading completion)
  - `elog` (error logging)
- Called from (representative examples):
  - Used internally by the node reading system for BoolExpr node deserialization
  - Typically invoked through the node reading dispatch mechanism

## Notes and Other Information
- This is a static function, part of the specialized node reading functions for complex expression nodes
- Uses manual string parsing instead of automated enum deserialization for the `boolop` field
- Essential for query plan deserialization, particularly for plans involving complex boolean logic
- Works in conjunction with `_outBoolExpr` in outfuncs.c for round-trip serialization
- The `args` field typically contains a list of expression nodes that are the operands of the boolean operation
- For NOT expressions, the `args` list typically contains a single element; for AND/OR expressions, it can contain multiple elements
- Critical component in PostgreSQL's expression evaluation system used throughout query processing