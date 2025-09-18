# check_simple_rowfilter_expr

## Location
src/backend/commands/publicationcmds.c: 590 - 604

## Overview
A wrapper function that initiates validation of publication WHERE clause expressions by calling the main tree walker validator.

## Definition
```c
static bool check_simple_rowfilter_expr(Node *node, ParseState *pstate)
```

## Detailed Description
This function serves as the entry point for validating publication row filter expressions. It provides a clean interface to the more complex check_simple_rowfilter_expr_walker function, which does the actual work of traversing the expression tree and enforcing the restrictions required for logical replication safety.

The function acts as a simple wrapper that delegates all validation logic to check_simple_rowfilter_expr_walker, maintaining separation of concerns between the public interface and the internal implementation details.

## Parameters / Member Variables
- `node`: The root node of the expression tree to validate
- `pstate`: Parse state used for error reporting and context

## Dependencies
- Functions called/Symbols referenced:
  - check_simple_rowfilter_expr_walker
- Called from:
  - TransformPubWhereClauses

## Notes and Other Information
- This is a static function used internally within publicationcmds.c
- The function returns the same boolean result as its walker counterpart
- Provides a cleaner API boundary for callers who don't need to know about the tree walking implementation
- All actual validation logic is implemented in check_simple_rowfilter_expr_walker