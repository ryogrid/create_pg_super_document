# ExplainOpenSetAsideGroup

## Location
src/backend/commands/explain.c: 4977 - 5015

## Overview
ExplainOpenSetAsideGroup prepares the formatting state for a group without emitting actual output, allowing subsequent output to be captured in a separate buffer and later appended to the main output.

## Definition


## Detailed Description
ExplainOpenSetAsideGroup is a specialized function that prepares the formatting state as if beginning a group with the specified properties, but without actually emitting any output. This enables a deferred output pattern where:

1. The formatting state is prepared (indentation, grouping stack)
2. Subsequent output can be redirected to a separate buffer
3. Later, the actual group can be opened with ExplainOpenGroup and the buffered content appended

The function handles format-specific state preparation:
- **TEXT format**: No action required
- **XML format**: Adjusts indentation by the specified depth
- **JSON format**: Pushes a placeholder (0) onto the grouping stack and adjusts indentation
- **YAML format**: Pushes appropriate grouping indicator (1 for labeled, 0 for unlabeled) onto the stack and adjusts indentation

The depth parameter allows for multi-level nesting scenarios where the eventual output will be enclosed in additional group levels.

## Parameters / Member Variables
- : The type of object for the group (used for identification, though not emitted)
- : The label name for the group (affects YAML grouping stack behavior)
- : Boolean flag indicating whether this is a labeled group
- : The nesting depth increase compared to current level (can be > 1 for multi-level nesting)
- : ExplainState structure containing formatting information and grouping stack

## Dependencies
- Functions called/Symbols referenced:
  - lcons_int (for managing the grouping stack in JSON and YAML formats)
- Called from (representative examples):
  - ExplainOpenWorker

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- There is no corresponding ExplainCloseSetAsideGroup function - the state is typically popped with ExplainSaveGroup
- Used in scenarios where output needs to be prepared and buffered before the actual group is opened
- The function is part of PostgreSQL's deferred output mechanism for complex EXPLAIN formatting
- Located in src/backend/commands/explain.c:4959-5003