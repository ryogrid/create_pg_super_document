# GetLWTrancheName

## Location
src/backend/storage/lmgr/lwlock.c: 745 - 768

## Overview
Returns the name of an LWLock tranche, handling both built-in tranches and user-defined extension tranches.

## Definition

```c
static const char *
GetLWTrancheName(uint16 trancheId)
```
## Detailed Description
GetLWTrancheName is a static function that retrieves the human-readable name of an LWLock tranche given its ID. It handles two categories of tranches:

1. **Built-in tranches**: For tranche IDs less than LWTRANCHE_FIRST_USER_DEFINED, it returns names from the BuiltinTrancheNames array.
2. **Extension tranches**: For user-defined tranches (IDs >= LWTRANCHE_FIRST_USER_DEFINED), it looks up names in the LWLockTrancheNames array. If the tranche hasn't been registered in the current process or is out of bounds, it returns a generic "extension" string.

This function is primarily used for debugging, logging, and diagnostic purposes to provide meaningful names for LWLock tranches in error messages and system information displays.

## Parameters / Member Variables
- `trancheId`: The unique identifier of the LWLock tranche whose name is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - LWTRANCHE_FIRST_USER_DEFINED (constant for boundary between built-in and user-defined tranches)
  - BuiltinTrancheNames (array containing names of built-in tranches)
  - LWLockTrancheNames (array containing names of extension tranches)
  - LWLockTrancheNamesAllocated (size of the LWLockTrancheNames array)

- Called from (representative examples):
  - T_NAME (macro for tranche name display)
  - print_lwlock_stats (statistics printing function)
  - GetLWLockIdentifier (identifier formatting function)

## Notes and Other Information
- This function is static, meaning it's only accessible within the lwlock.c file
- The function gracefully handles unregistered extension tranches by returning a generic "extension" name rather than failing
- Built-in tranche names are stored in a static array, while extension tranche names are dynamically allocated
- The function performs bounds checking to prevent array access violations when looking up extension tranche names