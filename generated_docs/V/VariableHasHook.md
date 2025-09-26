# VariableHasHook

## Location
src/bin/psql/variables.c: 367 - 391

## Overview
Checks whether a named variable has substitute and/or assign hook functions attached to it.

## Definition
```c
bool VariableHasHook(VariableSpace space, const char *name)
```

## Detailed Description
The VariableHasHook function is a utility function that determines whether a variable in the VariableSpace has any hook functions (substitute or assign hooks) attached to it. This is useful for determining if a variable has special behavior beyond simple value storage.

Key behaviors:
- Searches through the alphabetically ordered linked list of variables
- Returns true if the variable exists and has either a substitute hook or an assign hook (or both)
- Returns false if the variable doesn't exist or exists but has no hooks
- Uses efficient search taking advantage of the sorted order (breaks early when past the target name)
- Uses assertions to ensure valid input parameters

The function is primarily used to check if variables need special handling during operations like query result processing.

## Parameters / Member Variables
- `space`: VariableSpace (linked list head) to search in
- `name`: Name of the variable to check for hooks

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C string comparison)
  - Assert (PostgreSQL assertion macro)
- Data types referenced:
  - VariableSpace
  - struct _variable
- Called from (representative examples):
  - StoreQueryTuple (in src/bin/psql/common.c:789 - checks if query result variables have hooks)

## Notes and Other Information
- Returns true if either substitute_hook OR assign_hook is non-NULL
- Uses assertions rather than defensive programming - expects valid inputs
- Takes advantage of alphabetical ordering for efficient search
- Part of psql's variable system - used to determine if variables need special processing
- Simple boolean test - doesn't provide information about which specific hooks are present