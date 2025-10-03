# ExplainSaveGroup

## Location
[src/backend/commands/explain.c:5016-5045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5016-L5045)

## Overview
ExplainSaveGroup pops one level of grouping state while preserving the state information for later restoration, typically used in conjunction with ExplainOpenSetAsideGroup.

## Definition

```c
static void
ExplainSaveGroup(ExplainState *es, int depth, int *state_save)
```
## Detailed Description
ExplainSaveGroup is designed to temporarily save and pop formatting state that was previously prepared by ExplainOpenSetAsideGroup. This function:

1. Reduces the indentation level by the specified depth
2. For JSON and YAML formats, saves the top grouping stack item to the provided state_save location
3. Removes the top item from the grouping stack
4. Does not emit any output - it's purely a state management operation

This enables a pattern where formatting state can be temporarily set aside, content can be generated separately, and then the state can be restored later using ExplainRestoreGroup. The function handles format-specific state management:

- **TEXT format**: No action required
- **XML format**: Simply adjusts indentation
- **JSON/YAML formats**: Saves grouping information and manages the stack

## Parameters / Member Variables
- `*es`: ExplainState structure containing formatting information and grouping stack
- `depth`: The nesting depth to decrease (should match the depth used in ExplainOpenSetAsideGroup)
- `*state_save`: Pointer to integer storage where grouping state will be saved (for JSON/YAML formats)
## Dependencies
- Functions called/Symbols referenced:
  - linitial_int (to retrieve the first integer from the grouping stack)
  - [list_delete_first](../l/list_delete_first.md) (to remove the top item from the grouping stack)
- Called from (representative examples):
  - [ExplainCloseWorker](ExplainCloseWorker.md)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Typically used as part of a three-step process: ExplainOpenSetAsideGroup → ExplainSaveGroup → ExplainRestoreGroup
- The function does not emit any output - it's purely for state management
- Currently uses integer storage for saved state, though this may be extended in the future
- Must be paired with ExplainRestoreGroup to properly restore the saved state
- Located in src/backend/commands/explain.c:5005-5040

## Simplified Source

```c
static void
ExplainSaveGroup(ExplainState *es, int depth, int *state_save)
{
    switch (es->format)
    {
        case EXPLAIN_FORMAT_TEXT:
            // Text format requires no state management
            break;

        case EXPLAIN_FORMAT_XML:
            // XML only needs indentation adjustment
            es->indent -= depth;
            break;

        case EXPLAIN_FORMAT_JSON:
        case EXPLAIN_FORMAT_YAML:
            // JSON/YAML require full state management
            es->indent -= depth;
            *state_save = linitial_int(es->grouping_stack);
            es->grouping_stack = list_delete_first(es->grouping_stack);
            break;
    }
}
```