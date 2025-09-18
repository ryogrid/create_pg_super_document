# check_ssl

## Location
[src/backend/commands/variable.c:1249-1259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1249-L1259)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates whether SSL can be enabled based on the build configuration.

## Definition


## Detailed Description
The  function serves as a configuration validation hook for the SSL-related PostgreSQL configuration parameter. It performs a build-time check to ensure that SSL functionality is only enabled when PostgreSQL has been compiled with SSL support (USE_SSL macro defined). If a user attempts to enable SSL on a build that doesn't support it, the function generates an appropriate error message and prevents the invalid configuration.

This function follows the standard GUC check hook pattern, where it receives the proposed new value and validates whether it's acceptable given the current system state and build configuration.

## Parameters / Member Variables
- : Pointer to the proposed boolean value for the SSL configuration parameter
- : Pointer to extra data (unused in this implementation, can store additional validation context)
- : The source of the configuration change (e.g., configuration file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errmsg (for error reporting when SSL is not supported)
  - GucSource (enumeration type for configuration sources)
  - USE_SSL (build-time macro that indicates SSL support availability)
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H header file at src/include/utils/guc_hooks.h:138

## Notes and Other Information
- This function is conditionally compiled based on the USE_SSL macro - the error checking logic only exists when SSL support is not available
- When SSL support is available (USE_SSL is defined), the function always returns true, allowing any SSL configuration
- The function is part of PostgreSQL's configuration validation system, ensuring that users cannot enable features that aren't supported by their build
- Located in src/backend/commands/variable.c at lines 1249-1259
- Returns false only when SSL is requested but not supported by the build, otherwise returns true to allow the configuration change