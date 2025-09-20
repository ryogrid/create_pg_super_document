# check_log_timezone

## Location
[src/backend/commands/variable.c:416-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L416-L453)

## Overview
This is a GUC check hook function that validates timezone strings for the log_timezone configuration parameter in PostgreSQL.

## Definition

```c
bool
check_log_timezone(char **newval, void **extra, GucSource source)
```
## Detailed Description
The  function serves as the validation hook for PostgreSQL's  configuration parameter. It validates that the provided timezone string represents a valid timezone that PostgreSQL can use for logging purposes. Unlike the main  parameter,  does not support interval-based timezone methods, focusing only on named timezone support for SQL spec compliance.

The function attempts to load the timezone using  and performs additional validation to ensure the timezone is acceptable for PostgreSQL's use. If validation succeeds, it allocates memory to store the timezone information for the subsequent assignment operation.

## Parameters / Member Variables
- : Pointer to the new timezone string value to be validated
- : Pointer to store additional data (pg_tz structure) for the assign hook
- : The source of the GUC setting (not used in validation logic)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tzset](../p/pg_tzset.md)
  - [pg_tz_acceptable](../p/pg_tz_acceptable.md)  
  - GUC_check_errmsg
  - GUC_check_errdetail
  - [guc_malloc](../g/guc_malloc.md)
- Called from (representative examples):
  - PostgreSQL GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- This function does not support interval-based timezone methods, unlike the main timezone parameter
- Returns false if the timezone cannot be loaded or uses leap seconds (which PostgreSQL doesn't support)
- Memory allocated in the extra parameter is used by the corresponding assign_log_timezone function
- The function follows the standard GUC check hook pattern for PostgreSQL configuration validation