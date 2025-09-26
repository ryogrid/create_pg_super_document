# parse_comma_separated_list

## Location
[src/interfaces/libpq/fe-connect.c:1058-1092](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L1058-L1092)

## Overview
Parses a comma-separated list string iteratively, returning one malloc'd copy of each element per call and updating position tracking.

## Definition
```c
static char *parse_comma_separated_list(char **startptr, bool *more)
```

## Detailed Description
This function provides a stateful iterator for parsing comma-separated lists. It extracts one element at a time from a comma-separated string, returning a dynamically allocated copy of the current element. The function updates the caller's position pointer and indicates whether more elements remain. It handles the parsing by searching for the next comma delimiter or end-of-string, then creates a null-terminated copy of the element. The caller is responsible for freeing the returned memory.

## Parameters / Member Variables
- `startptr`: Pointer to a char pointer that tracks the current position in the list (updated after each call)
- `more`: Pointer to a boolean that indicates whether more elements remain after the current one

## Dependencies
- Functions called/Symbols referenced:
  - malloc (for allocating memory for the element copy)
  - memcpy (for copying the element data)
- Called from (representative examples):
  - [pqConnectOptions2](pqConnectOptions2.md) (multiple calls for parsing hostaddr, host, port, and other connection parameter lists)

## Notes and Other Information
- Returns a malloc'd copy of the next element, or NULL on out of memory
- Caller must free the returned string
- Updates *startptr to point to the character after the comma (or past end of string)
- Sets *more to true if there are more elements, false if this was the last element
- Does not handle escaped commas or quoted sections
- Assumes well-formed comma-separated input without leading/trailing whitespace handling
- Used in conjunction with count_comma_separated_elems for complete list processing
- Location: src/interfaces/libpq/fe-connect.c:1058-1092