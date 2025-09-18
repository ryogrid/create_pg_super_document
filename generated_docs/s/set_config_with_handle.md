# set_config_with_handle

## Location
src/backend/utils/misc/guc.c: 3408 - 3711

## Overview
Sets a configuration option to a given value with optional handle optimization for repeated settings of the same option.

## Definition


## Detailed Description
This function is the core implementation for setting PostgreSQL configuration options (GUCs). It provides an optimized interface that accepts a handle parameter to avoid repeated hash table lookups when setting the same configuration option multiple times. The function performs comprehensive validation of the setting request including context checks, privilege verification, and parallel operation safety.

The function handles various configuration contexts (POSTMASTER, SIGHUP, BACKEND, etc.) and enforces appropriate restrictions based on the parameter's definition and the current execution environment. It supports different actions like setting, saving for transaction rollback, and handles security restrictions appropriately.

## Parameters / Member Variables
- : Name of the configuration parameter to set
- : Optional handle from get_config_handle() to avoid hash lookup (NULL for normal lookup)
- : String value to set the parameter to (NULL for reset)
- : Context in which the setting is being made (PGC_INTERNAL, PGC_POSTMASTER, etc.)
- : Source of the setting (file, command line, etc.)
- : Role ID for privilege checking
- : Action to perform (GUC_ACTION_SET, GUC_ACTION_SAVE, etc.)
- : Whether to actually change the value or just validate
- : Error level for reporting problems
- : Whether this is part of a configuration reload

## Dependencies
- Functions called/Symbols referenced:
  - find_option
  - IsInParallelMode
  - pg_parameter_aclcheck
  - InLocalUserIdChange
  - InSecurityRestrictedOperation
  - config_generic
  - GucContext, GucSource, GucAction enums
- Called from (representative examples):
  - set_config_option
  - set_config_option_ext
  - fmgr_security_definer

## Notes and Other Information
- Returns 1 on success, 0 on failure, -1 if setting was ignored due to lower priority source
- The handle parameter is designed for performance optimization when repeatedly setting the same configuration options
- Implements comprehensive security checks including parallel operation restrictions and privilege validation
- Handles different parameter contexts with appropriate validation and error reporting
- Part of PostgreSQL's Grand Unified Configuration (GUC) system located in src/backend/utils/misc/guc.c:3408-3711