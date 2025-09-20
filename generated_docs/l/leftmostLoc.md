# leftmostLoc

## Location
[src/backend/nodes/nodeFuncs.c:1810-1830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L1810-L1830)

## Overview
A utility function that determines the leftmost (minimum) parse location between two location values, ignoring unknown locations (-1).

## Definition

```c
static int
leftmostLoc(int loc1, int loc2)
```
## Detailed Description
The  function is a simple but essential helper function used by  to determine the leftmost position among multiple parse location candidates. It implements logic to handle unknown locations gracefully while finding the true start position of complex expressions.

The function follows these rules:
1. If  is unknown (< 0), return  regardless of its value
2. If  is unknown (< 0), return  regardless of its value  
3. If both locations are valid (>= 0), return the minimum (leftmost) position

This logic is crucial for 's operation because parse tree nodes created during analysis may have location -1, while others have valid positions. The function allows  to find the best available location information even when some components lack position data.

## Parameters
- : First parse location value (int, may be -1 for unknown)
- : Second parse location value (int, may be -1 for unknown)

## Dependencies
- Functions called/Symbols referenced:
  - Min (macro for finding minimum of two values)

- Called from:
  - [exprLocation](../e/exprLocation.md) (extensively throughout src/backend/nodes/nodeFuncs.c, lines 1426-1699)

## Notes and Other Information
- Declared as static, so it's only visible within nodeFuncs.c
- Essential support function for PostgreSQL's error location reporting system
- Handles the common case where parse analysis creates nodes without location information
- Simple but critical for providing meaningful error messages to users
- Part of the location tracking infrastructure used throughout the parser
- Always returns a valid location if at least one input is valid, or -1 if both are unknown
- Located in src/backend/nodes/nodeFuncs.c:1810-1830