# injection_points_detach

## Location
src/test/modules/injection_points/injection_points.c: 386 - 404

## Overview
This function detaches a named injection point and removes it from both the global system and the local process tracking list.

## Definition
```c
Datum injection_points_detach(PG_FUNCTION_ARGS)
```

## Detailed Description
The `injection_points_detach` function removes a previously attached injection point from the PostgreSQL injection point testing framework. It takes the name of an injection point as a text argument, converts it to a C string, and calls the core `InjectionPointDetach` function to remove it from the global system. Additionally, if the injection point was tracked locally (in `inj_list_local`), it removes the entry from the local tracking list. The function ensures proper memory context management when modifying the local list and throws an error if the detachment operation fails.

This function is used in testing scenarios to clean up injection points that are no longer needed, ensuring that test environments remain clean and don't interfere with subsequent tests.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Text argument containing the name of the injection point to detach

## Dependencies
- Functions called/Symbols referenced:
  - `text_to_cstring`
  - `[InjectionPointDetach](../I/InjectionPointDetach.md)`
  - `[list_delete](../l/list_delete.md)`
  - `[makeString](../m/makeString.md)`
  - `PG_RETURN_VOID`
- Called from (representative examples):
  - Referenced by `injection_points_set_local` (src/test/modules/injection_points/injection_points.c:384)

## Notes and Other Information
- Validates the detachment operation and throws an ERROR if the injection point could not be detached
- Manages memory context switching to TopMemoryContext when modifying the local injection point list
- Removes entries from the local tracking list (`inj_list_local`) if they exist
- Part of the PostgreSQL test infrastructure, located in src/test/modules/injection_points/
- Returns void through the PostgreSQL function interface
- Works in conjunction with the injection point cleanup system to maintain consistent state