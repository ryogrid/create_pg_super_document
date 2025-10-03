# check_bonjour

## Location
[src/backend/commands/variable.c:1195-1207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1195-L1207)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates attempts to enable Bonjour service discovery, rejecting the setting if PostgreSQL was not compiled with Bonjour support.

## Definition

```c
bool
check_bonjour(bool *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a check hook for the bonjour GUC parameter in PostgreSQL. Check hooks are validation functions called by PostgreSQL's configuration system before a parameter value is accepted. The function ensures that users cannot enable Bonjour service discovery on PostgreSQL builds that were compiled without USE_BONJOUR support.

When Bonjour support is not compiled in (USE_BONJOUR is not defined), the function will reject any attempt to set the parameter to true by returning false and setting an appropriate error message. This prevents runtime errors and provides clear feedback to users about build limitations.

## Parameters / Member Variables
- `*newval`: Pointer to the new boolean value being set for the bonjour parameter
- `**extra`: Pointer to extra data (unused in this function, can be NULL)
- `source`: The source of the configuration change (GucSource enum value)
## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errmsg (macro for setting GUC error messages)
  - GucSource (enum type for configuration sources)
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H header file for GUC system integration

## Notes and Other Information
- This is a compile-time conditional check hook that only rejects settings when USE_BONJOUR is not defined
- Part of a family of similar check hooks that validate build-specific features
- Returns true to accept the new value, false to reject it
- Uses the GUC_check_errmsg macro to provide user-friendly error messages
- Bonjour is Apple's implementation of zero-configuration networking (Zeroconf)