# get_config_handle

## Location
[src/backend/utils/misc/guc.c:4287-4301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4287-L4301)

## Overview
Retrieves a configuration handle for a given parameter name to optimize repeated calls to set_config_with_handle().

## Definition


## Detailed Description
This function provides a way to obtain a handle (pointer) to a configuration option's internal structure for performance optimization. The returned handle can be passed to set_config_with_handle() to avoid the overhead of repeated hash table lookups when setting the same configuration parameter multiple times. The function only returns handles for permanent (non-placeholder) GUC parameters to ensure stability.

This is particularly useful in scenarios where the same configuration option needs to be set repeatedly, such as in function calls with parameter overrides or batch configuration operations.

## Parameters / Member Variables
- : Name of the configuration parameter to get a handle for

## Dependencies
- Functions called/Symbols referenced:
  - find_option
  - config_generic
  - GUC_CUSTOM_PLACEHOLDER
- Called from (representative examples):
  - [fmgr_security_definer](../f/fmgr_security_definer.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Returns a config_handle pointer on success, NULL if parameter not found or is a placeholder
- Only returns handles for permanent GUC parameters (excludes custom placeholders)
- The returned handle remains valid as long as the GUC system is initialized
- Designed to work in conjunction with set_config_with_handle() for performance optimization
- Located in src/backend/utils/misc/guc.c:4287-4301