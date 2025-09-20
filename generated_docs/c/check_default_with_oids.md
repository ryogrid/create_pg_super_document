# check_default_with_oids

## Location
[src/backend/commands/variable.c:1208-1222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1208-L1222)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates attempts to enable the default_with_oids parameter, unconditionally rejecting it since WITH OIDS tables are no longer supported in PostgreSQL.

## Definition

```c
bool
check_default_with_oids(bool *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a check hook for the default_with_oids GUC parameter in PostgreSQL. Check hooks are validation functions called by PostgreSQL's configuration system before a parameter value is accepted. This particular function implements a complete rejection policy for the default_with_oids setting, as PostgreSQL has removed support for tables declared WITH OIDS.

The function will reject any attempt to set the parameter to true, regardless of the source of the configuration change. It provides a clear error message indicating that tables declared WITH OIDS are not supported, along with an appropriate SQL error code (ERRCODE_FEATURE_NOT_SUPPORTED).

## Parameters / Member Variables
- : Pointer to the new boolean value being set for the default_with_oids parameter
- : Pointer to extra data (unused in this function, can be NULL)
- : The source of the configuration change (GucSource enum value)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errcode (macro for setting GUC error codes)
  - GUC_check_errmsg (macro for setting GUC error messages)
  - GucSource (enum type for configuration sources)
  - ERRCODE_FEATURE_NOT_SUPPORTED (error code constant)
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H header file for GUC system integration

## Notes and Other Information
- This function always rejects attempts to enable default_with_oids, reflecting PostgreSQL's removal of OID support
- WITH OIDS tables were deprecated and removed in PostgreSQL 12
- Uses both error code and error message macros to provide comprehensive error reporting
- Returns true to accept the new value (only when false), false to reject it (always when true)
- The comment refers to the GUC definition for historical context about this feature's removal