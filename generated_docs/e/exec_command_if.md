# exec_command_if

## Location
src/bin/psql/command.c: 1743 - 1788

## Overview
Implements the \if command in PostgreSQL's psql client for conditional execution of SQL commands based on boolean expressions.

## Definition


## Detailed Description
This function handles the execution of the \if backslash command in psql, which begins a conditional block. It evaluates a boolean expression and determines whether subsequent commands should be executed or ignored until the matching \endif. The function manages a conditional stack to track nested \if..\endif blocks and their states (true, false, or ignored). When in an active branch, it evaluates the expression and sets the appropriate state. When already in an inactive branch, the entire inner \if block is ignored without evaluating the expression. The function also saves the current query state to enable proper restoration when exiting the conditional block.

## Parameters / Member Variables
- : PsqlScanState pointer that tracks the current parsing state of the psql input
- : ConditionalStack managing the hierarchy of nested conditional blocks
- : PQExpBuffer containing the current query text that may need restoration

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_active](../c/conditional_active.md) (checks if currently in an active conditional branch)
  - [conditional_stack_push](../c/conditional_stack_push.md) (adds new conditional state to stack)
  - [conditional_stack_poke](../c/conditional_stack_poke.md) (modifies top conditional state)
  - [save_query_text_state](../s/save_query_text_state.md) (preserves current query state for restoration)
  - [is_true_boolean_expression](../i/is_true_boolean_expression.md) (evaluates the boolean expression)
  - [ignore_boolean_expression](../i/ignore_boolean_expression.md) (skips expression parsing when in inactive branch)
  - IFSTATE_TRUE, IFSTATE_FALSE, IFSTATE_IGNORED (conditional state constants)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher in psql)

## Notes and Other Information
- Always returns PSQL_CMD_SKIP_LINE as conditional commands don't produce immediate output
- Supports nested \if..\endif blocks through the conditional stack mechanism
- [Variable](../V/Variable.md) substitution and backtick evaluation occur during expression parsing in active branches
- Invalid boolean expressions emit warnings and are treated as false
- Forms the foundation of psql's conditional scripting capabilities along with \elif, \else, and \endif
- The query buffer state is saved to handle cases where conditional blocks span multiple lines