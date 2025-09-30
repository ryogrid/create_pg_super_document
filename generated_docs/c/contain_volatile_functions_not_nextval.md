# contain_volatile_functions_not_nextval

## Location
[src/backend/optimizer/util/clauses.c:673-678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L673-L678)

## Overview
A specialized version of volatile function detection designed for COPY operations that ignores nextval() calls while treating all other functions normally.

## Definition
```c
bool contain_volatile_functions_not_nextval(Node *clause)
```

## Detailed Description
This function provides a specialized variant of volatile function checking specifically tailored for COPY command processing. Unlike the standard `contain_volatile_functions()`, this version deliberately ignores `nextval()` function calls while maintaining normal volatility checking for all other functions.

The special handling of `nextval()` is important in COPY contexts because sequence operations in COPY commands may have different semantic requirements compared to regular query processing. The function delegates the actual tree walking and checking to `contain_volatile_functions_not_nextval_walker()`.

## Parameters / Member Variables
- `clause`: The node tree to analyze for volatile function content (excluding nextval)

## Dependencies
- Functions called/Symbols referenced:
  - [contain_volatile_functions_not_nextval_walker](contain_volatile_functions_not_nextval_walker.md): Performs the actual tree walking and volatility checking
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (at copyfrom.c:1676)
  - [DebugParallelMode](../D/DebugParallelMode.md) (referenced in optimizer.h:145)

## Notes and Other Information
- Specifically designed for use in COPY operations
- The special treatment of nextval() reflects different semantic requirements in COPY contexts
- Returns a boolean indicating whether volatile functions (other than nextval) are present
- Part of the specialized volatility checking infrastructure for bulk operations

## Simplified Source
```c
bool contain_volatile_functions_not_nextval(Node *clause) {
    // Delegate to walker function that ignores nextval() but checks other volatile functions
    return contain_volatile_functions_not_nextval_walker(clause, NULL);
}
```