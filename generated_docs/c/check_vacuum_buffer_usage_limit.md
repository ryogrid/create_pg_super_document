# check_vacuum_buffer_usage_limit

## Location
[src/backend/commands/vacuum.c:126-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L126-L147)

## Overview
A GUC (Grand Unified Configuration) check function that validates the vacuum_buffer_usage_limit configuration parameter to ensure it falls within acceptable ranges.

## Definition

```c
bool
check_vacuum_buffer_usage_limit(int *newval, void **extra,
								GucSource source)
```
## Detailed Description
This function serves as a validation hook for PostgreSQL's GUC system, specifically for the vacuum_buffer_usage_limit parameter. It ensures that any value assigned to this configuration parameter is either 0 (unlimited) or falls within the predefined acceptable range for vacuum buffer ring sizes. The function is called automatically by the GUC system whenever an attempt is made to set the vacuum_buffer_usage_limit parameter, providing runtime validation to prevent invalid configurations.

The function implements a dual validation logic: it accepts a value of 0 (which typically means unlimited or disabled) or any value between the minimum and maximum buffer allocation service vacuum ring size constants. If the value falls outside these acceptable ranges, it generates a detailed error message indicating the valid ranges.

## Parameters / Member Variables
- : Pointer to the integer value being validated for the vacuum_buffer_usage_limit parameter
- : Pointer to extra data that can be passed to the check function (unused in this implementation)
- : The source from which the GUC value is being set (e.g., configuration file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail
  - MIN_BAS_VAC_RING_SIZE_KB (constant)
  - MAX_BAS_VAC_RING_SIZE_KB (constant)
  - GucSource (type)
- Called from (representative examples):
  - GUC system hooks (referenced in guc_hooks.h)

## Notes and Other Information
- The function accepts 0 as a special value, typically indicating unlimited buffer usage
- Valid non-zero values must be between MIN_BAS_VAC_RING_SIZE_KB and MAX_BAS_VAC_RING_SIZE_KB (inclusive)
- Uses GUC_check_errdetail to provide user-friendly error messages when validation fails
- This is part of PostgreSQL's configuration validation infrastructure, ensuring system stability by preventing invalid buffer size configurations
- The function follows the standard GUC check function signature pattern used throughout PostgreSQL