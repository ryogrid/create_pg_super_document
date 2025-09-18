# exec_command_elif

## Location
src/bin/psql/command.c: 1789 - 1864

## Overview
Implements the \elif command in PostgreSQL's psql client for providing alternative conditional branches within \if..\endif blocks.

## Definition


## Detailed Description
This function handles the execution of the \elif (else if) backslash command in psql, which provides alternative conditional branches within an \if..\endif block. The function's behavior depends on the current state of the conditional stack: if the previous branch was true, it ignores the expression and remaining branches; if false, it evaluates the new expression; if already ignored, it continues ignoring. The function validates that \elif appears in the correct context (after \if but before any \else) and manages query buffer state appropriately by either preserving text from active branches or discarding text from inactive ones.

## Parameters / Member Variables
- : PsqlScanState pointer that tracks the current parsing state of the psql input
- : ConditionalStack managing the hierarchy of nested conditional blocks
- : PQExpBuffer containing the current query text that may need preservation or disposal

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_peek](../c/conditional_stack_peek.md) (examines current conditional state without modifying stack)
  - [conditional_stack_poke](../c/conditional_stack_poke.md) (modifies the top conditional state)
  - [save_query_text_state](../s/save_query_text_state.md) (preserves query state from active branches)
  - [discard_query_text](../d/discard_query_text.md) (removes query text from inactive branches)
  - [is_true_boolean_expression](../i/is_true_boolean_expression.md) (evaluates the elif boolean expression)
  - [ignore_boolean_expression](../i/ignore_boolean_expression.md) (skips expression parsing when ignoring)
  - pg_log_error (reports syntax and context errors)
  - IFSTATE_TRUE, IFSTATE_FALSE, IFSTATE_IGNORED, IFSTATE_ELSE_TRUE, IFSTATE_ELSE_FALSE, IFSTATE_NONE (conditional state constants)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher in psql)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure
- Validates proper usage context - reports errors for \elif after \else or without preceding \if
- Supports multiple \elif branches within a single \if..\endif block
- Uses a state machine approach to handle different conditional states appropriately
- Part of psql's conditional scripting system enabling complex branching logic in SQL scripts
- [Query](../Q/Query.md) buffer management ensures that only text from the single active branch is preserved
- Expression evaluation only occurs when transitioning from a false state to potentially true state