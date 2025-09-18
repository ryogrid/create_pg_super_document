# conditional_active

## Location
src/fe_utils/conditional.c: 140 - 150

## Overview
Determines whether commands should execute normally by checking if the current conditional branch is active or if there are no open conditional blocks.

## Definition
```c
bool conditional_active(ConditionalStack cstack)
```

## Detailed Description
This function is the primary decision point for whether commands should be executed in the context of conditional processing. It uses conditional_stack_peek() to get the current state and returns true if: (1) there are no active conditional blocks (IFSTATE_NONE), (2) the current conditional branch is true (IFSTATE_TRUE), or (3) an else branch is active and true (IFSTATE_ELSE_TRUE). This effectively determines when PostgreSQL frontend utilities should process commands normally versus skipping them due to inactive conditional branches.

## Parameters / Member Variables
- `cstack`: A ConditionalStack pointer representing the conditional stack to check for active state

## Dependencies
- Functions called/Symbols referenced:
  - conditional_stack_peek (function)
  - ifState (typedef)
  - ConditionalStack (typedef)
  - IFSTATE_NONE (enum value)
  - IFSTATE_TRUE (enum value)  
  - IFSTATE_ELSE_TRUE (enum value)
- Called from (representative examples):
  - advanceConnectionState (pgbench) - multiple locations
  - HandleSlashCmds (psql)
  - exec_command (psql)
  - exec_command_if (psql)
  - psql_get_variable (psql)
  - MainLoop (psql) - multiple locations
  - MAX_PROMPT_SIZE (psql)

## Notes and Other Information
- This is the central function that controls command execution flow in conditional processing
- Returns true when commands should execute, false when they should be skipped
- Handles the case where no conditional blocks are active (always execute)
- Critical for implementing proper \if/\elif/\else/\endif behavior in PostgreSQL frontend tools
- Used extensively throughout psql and pgbench for conditional command execution