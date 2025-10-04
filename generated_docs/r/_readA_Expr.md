# _readA_Expr

## Location
[src/backend/nodes/readfuncs.c:439-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/readfuncs.c#L439-L525)

## Overview
A static function that deserializes an A_Expr node from its string representation, handling various types of SQL expressions and operators.

## Definition

```c
static A_Expr *
_readA_Expr(void)
```
## Detailed Description
The  function reconstructs A_Expr nodes from their serialized format during PostgreSQL's node deserialization process. An A_Expr represents an expression in the parse tree, particularly binary and unary operators, comparison operations, and special SQL constructs.

The function reads a token to determine the expression kind and then sets the appropriate  field along with reading the associated node fields. It handles the following expression types:
- **AEXPR_OP_ANY**: ANY operator expressions (e.g., col = ANY(array))
- **AEXPR_OP_ALL**: ALL operator expressions (e.g., col > ALL(array))
- **AEXPR_DISTINCT**: IS DISTINCT FROM operations
- **AEXPR_NOT_DISTINCT**: IS NOT DISTINCT FROM operations
- **AEXPR_NULLIF**: NULLIF function expressions
- **AEXPR_IN**: IN operator expressions
- **AEXPR_LIKE**: LIKE pattern matching
- **AEXPR_ILIKE**: ILIKE (case-insensitive LIKE)
- **AEXPR_SIMILAR**: SIMILAR TO pattern matching
- **AEXPR_BETWEEN**: BETWEEN range checks
- **AEXPR_NOT_BETWEEN**: NOT BETWEEN range checks
- **AEXPR_BETWEEN_SYM**: Symmetric BETWEEN (order doesn't matter)
- **AEXPR_NOT_BETWEEN_SYM**: NOT BETWEEN SYMMETRIC
- **AEXPR_OP**: Generic binary/unary operators

For most expression types, the function reads a predefined name field, but for generic operators (AEXPR_OP), it uses nodeRead to deserialize the actual operator name.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - READ_LOCALS (macro for local variable setup)
  - [pg_strtok](../p/pg_strtok.md) (tokenizer function)
  - READ_NODE_FIELD (macro to read node fields)
  - [nodeRead](../n/nodeRead.md) (generic node reading function)
  - READ_LOCATION_FIELD (macro to read location information)
  - READ_DONE (macro for cleanup)
  - elog (error logging function)
  - AEXPR_* constants (expression kind enums)
- Called from (representative examples):
  - No direct references found (likely called via function pointer table)

## Notes and Other Information
- This is a static function, accessible only within readfuncs.c
- Uses extensive string comparison logic to determine expression type from serialized tokens
- Handles 14 different types of A_Expr kinds with specialized processing for each
- Generic operators (AEXPR_OP) are identified by the ":name" token and require dynamic name reading
- Always reads left expression (lexpr), right expression (rexpr), and location information regardless of expression type
- Part of PostgreSQL's expression tree deserialization system used for query plan caching and parallel execution
- Error handling includes the actual unrecognized token in the error message for debugging

## Simplified Source

```c
static A_Expr *
_readA_Expr(void)
{
    READ_LOCALS(A_Expr);

    token = pg_strtok(&length);

    // Parse expression kind from token
    if (length == 3 && strncmp(token, "ANY", 3) == 0) {
        local_node->kind = AEXPR_OP_ANY;
        READ_NODE_FIELD(name);
    } else if (length == 3 && strncmp(token, "ALL", 3) == 0) {
        local_node->kind = AEXPR_OP_ALL;
        READ_NODE_FIELD(name);
    } else if (length == 8 && strncmp(token, "DISTINCT", 8) == 0) {
        local_node->kind = AEXPR_DISTINCT;
        READ_NODE_FIELD(name);
    } else if (length == 12 && strncmp(token, "NOT_DISTINCT", 12) == 0) {
        local_node->kind = AEXPR_NOT_DISTINCT;
        READ_NODE_FIELD(name);
    } else if (length == 6 && strncmp(token, "NULLIF", 6) == 0) {
        local_node->kind = AEXPR_NULLIF;
        READ_NODE_FIELD(name);
    } else if (length == 2 && strncmp(token, "IN", 2) == 0) {
        local_node->kind = AEXPR_IN;
        READ_NODE_FIELD(name);
    } else if (length == 4 && strncmp(token, "LIKE", 4) == 0) {
        local_node->kind = AEXPR_LIKE;
        READ_NODE_FIELD(name);
    } else if (length == 5 && strncmp(token, "ILIKE", 5) == 0) {
        local_node->kind = AEXPR_ILIKE;
        READ_NODE_FIELD(name);
    } else if (length == 7 && strncmp(token, "SIMILAR", 7) == 0) {
        local_node->kind = AEXPR_SIMILAR;
        READ_NODE_FIELD(name);
    } else if (length == 7 && strncmp(token, "BETWEEN", 7) == 0) {
        local_node->kind = AEXPR_BETWEEN;
        READ_NODE_FIELD(name);
    } else if (length == 11 && strncmp(token, "NOT_BETWEEN", 11) == 0) {
        local_node->kind = AEXPR_NOT_BETWEEN;
        READ_NODE_FIELD(name);
    } else if (length == 5 && strncmp(token, ":name", 5) == 0) {
        local_node->kind = AEXPR_OP;
        local_node->name = nodeRead(NULL, 0);
    } else {
        elog(ERROR, "unrecognized A_Expr kind: \"%.*s\"", length, token);
    }

    // Read left and right expressions
    READ_NODE_FIELD(lexpr);
    READ_NODE_FIELD(rexpr);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}
```