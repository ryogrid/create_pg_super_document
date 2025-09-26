# from_char_set_int

## Location
src/backend/utils/adt/formatting.c: 2427 - 2462

## Overview
A utility function that safely sets an integer value while preventing conflicting assignments during date/time parsing operations.

## Definition
```c
static bool from_char_set_int(int *dest, const int value, const FormatNode *node, Node *escontext)
```

## Detailed Description
This function provides a safe mechanism to set integer values during PostgreSQL's date/time formatting and parsing operations. It validates that the destination integer hasn't been previously set to a different non-zero value, preventing conflicting field assignments that could lead to ambiguous or invalid dates/times. The function uses PostgreSQL's soft error handling mechanism through the escontext parameter, allowing for graceful error recovery in parsing operations.

## Parameters / Member Variables
- `dest`: Pointer to the destination integer to be set
- `value`: The integer value to assign to the destination
- `node`: Pointer to FormatNode containing formatting context information (used for error reporting)
- `escontext`: Node pointer for error context handling, enables soft error reporting when not NULL

## Dependencies
- Functions called/Symbols referenced:
  - FormatNode (struct type)
  - ereturn (PostgreSQL error handling macro)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1058)
  - from_char_parse_int_len (formatting.c:2543)
  - DCH_from_char (multiple locations: 3543, 3555, 3722, 3733, 3744, 3755, 3771, 3783, 3855, 3905)

## Notes and Other Information
- Returns true on success, false on failure when using soft error handling
- Allows overwriting with the same value (no conflict when *dest == value)
- Allows setting zero values to any non-zero value (initial assignment)
- Provides descriptive error messages when conflicts are detected
- Part of PostgreSQL's robust date/time parsing validation infrastructure
- Critical for ensuring data integrity during complex formatting template processing