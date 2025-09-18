# CheckConditional

## Location
[src/bin/pgbench/pgbench.c:5891-5940](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5891-L5940)

## Overview
Performs static validation of conditional statement structure (if/elif/else/endif) in pgbench scripts to ensure proper nesting and syntax.

## Definition
static void CheckConditional(const ParsedScript *ps)

## Detailed Description
This function validates the syntactic correctness of conditional constructs in a parsed pgbench script by maintaining a conditional stack to track nesting levels and states. It iterates through all commands in the script, processing META_IF, META_ELIF, META_ELSE, and META_ENDIF commands to ensure they are properly paired and nested. The function detects various syntax errors such as unmatched if/endif pairs, elif after else, else after else, and orphaned conditional statements. This validation occurs before script execution to catch structural errors early.

## Parameters / Member Variables
- ps: Pointer to ParsedScript structure containing the parsed commands to validate

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_create](../c/conditional_stack_create.md), conditional_stack_destroy
  - [conditional_stack_push](../c/conditional_stack_push.md), conditional_stack_pop, conditional_stack_poke
  - [conditional_stack_empty](../c/conditional_stack_empty.md), conditional_stack_peek
  - [ConditionError](ConditionError.md)
  - [ParsedScript](../P/ParsedScript.md), ConditionalStack, Command structs
  - META_COMMAND, META_IF, META_ELIF, META_ELSE, META_ENDIF enums
  - IFSTATE_FALSE, IFSTATE_ELSE_FALSE enum values
- Called from:
  - [addScript](../a/addScript.md) (src/bin/pgbench/pgbench.c:6237)

## Notes and Other Information
- Uses a stack-based approach to track nested conditional levels
- Validates conditional structure statically before runtime execution
- Reports specific error messages for different types of conditional syntax errors
- Part of pgbench's script validation pipeline
- Ensures that every if has a matching endif and proper nesting
- Detects illegal sequences like elif after else or multiple else clauses
- Called during script loading/preparation phase, not during execution