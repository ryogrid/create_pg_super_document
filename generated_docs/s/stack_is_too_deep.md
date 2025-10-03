# stack_is_too_deep

## Location
[src/backend/tcop/postgres.c:3572-3604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3572-L3604)

## Overview
stack_is_too_deep determines whether the current call stack depth exceeds the configured safety limit, providing a boolean check for stack overflow prevention without immediately throwing an error.

## Definition

```c
bool
stack_is_too_deep(void)
```
## Detailed Description
stack_is_too_deep performs the actual stack depth calculation and comparison to determine if the current recursion level has exceeded the safe threshold. Unlike check_stack_depth(), this function returns a boolean result rather than immediately throwing an error, allowing calling code to handle the stack depth condition as appropriate.

The function works by:
1. Creating a local variable (stack_top_loc) to represent the current stack position
2. Calculating the distance between the stack base reference point (stack_base_ptr) and the current position
3. Taking the absolute value of the distance since stacks can grow upward or downward depending on the platform
4. Comparing the calculated depth against max_stack_depth_bytes
5. Returning true if the depth exceeds the limit and the stack base pointer is valid

The check for stack_base_ptr != NULL prevents false positives during process setup or in non-backend processes where the stack base may not have been established yet. This check is performed after the depth calculation for performance optimization during normal operation.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - stack_base_ptr (global variable - stack base reference point)
  - max_stack_depth_bytes (configuration parameter in bytes)
- Called from (representative examples):
  - [check_stack_depth](../c/check_stack_depth.md)
  - [ShowTransactionStateRec](../S/ShowTransactionStateRec.md)
  - [rstacktoodeep](../r/rstacktoodeep.md) (regex compilation)
  - [MemoryContextStatsInternal](../M/MemoryContextStatsInternal.md)

## Notes and Other Information
- Platform-independent implementation that handles both upward and downward growing stacks
- Provides flexibility for code that wants to handle stack depth conditions rather than immediately erroring
- Performance-optimized by placing the stack_base_ptr check after the depth calculation
- Used both directly and indirectly through check_stack_depth() throughout the PostgreSQL codebase
- The max_stack_depth_bytes value is derived from the max_stack_depth configuration parameter
- Critical component of PostgreSQL's stack overflow prevention system

## Simplified Source

```c
// Simplified version of stack_is_too_deep
bool stack_is_too_deep(void) {
    char stack_top_loc;
    long stack_depth;

    // Calculate distance from stack base to current position
    stack_depth = (long)(stack_base_ptr - &stack_top_loc);

    // Handle both upward and downward growing stacks
    if (stack_depth < 0) {
        stack_depth = -stack_depth;
    }

    // Check if stack exceeds limit (only when stack base is initialized)
    if (stack_depth > max_stack_depth_bytes && stack_base_ptr != NULL) {
        return true;
    }

    return false;
}
```

Key simplifications made:
- Preserved core stack depth calculation logic
- Maintained platform-independent stack growth handling
- Kept essential safety check for stack_base_ptr
- Simplified comments to focus on main algorithm steps
- Removed detailed comment explanations while preserving functionality