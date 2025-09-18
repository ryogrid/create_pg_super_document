# param_is_newly_set

## Location
[src/bin/psql/command.c:3363-3385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3363-L3385)

## Overview
Determines whether a parameter has been given a new value by comparing old and new string values.

## Definition
```c
static bool param_is_newly_set(const char *old_val, const char *new_val)
```

## Detailed Description
This utility function compares two string parameters to determine if a parameter has been assigned a new value. It handles the common case where connection parameters might be NULL (unset) or have string values. The function returns true if the new value is different from the old value, including cases where the old value was NULL and a new value is now provided. It returns false if the new value is NULL or if both values are identical.

## Parameters / Member Variables
- `old_val`: The previous value of the parameter (can be NULL)
- `new_val`: The new value being assigned to the parameter (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
- Called from (representative examples):
  - [do_connect](../d/do_connect.md)

## Notes and Other Information
- This is a static function used internally within psql's connection handling
- Used to detect when connection parameters have changed during connection operations
- Handles NULL values safely - treats NULL as a distinct value
- Returns false immediately if new_val is NULL (no new value being set)
- Returns true if old_val was NULL but new_val is not NULL (parameter being set for first time)
- Returns true if both values are non-NULL but different according to strcmp
- Part of psql's connection parameter management infrastructure