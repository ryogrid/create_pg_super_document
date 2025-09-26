# InitializeGUCOptionsFromEnvironment

## Location
[src/backend/utils/misc/guc.c:1591-1645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1591-L1645)

## Overview
Applies GUC parameter values from environment variables and system resource limits, providing historical compatibility and automatic tuning for stack depth limits.

## Definition
```c
static void InitializeGUCOptionsFromEnvironment(void)
```

## Detailed Description
This function processes environment variables and system resource limits to set initial values for specific GUC parameters, maintaining compatibility with historical PostgreSQL deployment practices. The function handles several categories of environment-based configuration:

For traditional environment variables, it checks and applies values from PGPORT (for the port parameter), PGDATESTYLE (for datestyle), and PGCLIENTENCODING (for client_encoding). These environment variables provide a legacy mechanism for setting common configuration parameters.

The function also implements intelligent stack depth management by querying the system's stack size resource limit (rlimit). It calculates a safe maximum stack depth value that doesn't exceed the system limit, accounting for safety margins (STACK_DEPTH_SLOP). The calculated value is capped at 2MB and marked with an appropriate source designation (PGC_S_ENV_VAR if limited by rlimit, PGC_S_DYNAMIC_DEFAULT if capped at 2MB).

This dual approach ensures both backward compatibility with existing deployment scripts and automatic optimization based on system capabilities.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - getenv: Standard library function to read environment variables
  - [SetConfigOption](../S/SetConfigOption.md): Sets GUC parameter values with specified context and source
  - [get_stack_depth_rlimit](../g/get_stack_depth_rlimit.md): Retrieves system stack size limit
  - snprintf: Formats numeric values as strings
  - PGC_POSTMASTER: Configuration context constant
  - PGC_S_ENV_VAR, PGC_S_DYNAMIC_DEFAULT: Configuration source constants
  - STACK_DEPTH_SLOP: Safety margin constant for stack calculations
  - GucSource: Type for configuration source tracking
- Called from (representative examples):
  - [InitializeGUCOptions](InitializeGUCOptions.md): Main GUC initialization during startup
  - ProcessConfigFile: Configuration reload processing (legacy usage)

## Notes and Other Information
- Static function used only within the GUC subsystem
- Supports legacy deployment practices through environment variable processing
- Automatically tunes max_stack_depth based on system capabilities
- The comment suggests this mechanism may be deprecated in favor of configuration files
- Environment variables processed: PGPORT, PGDATESTYLE, PGCLIENTENCODING
- Stack depth calculation includes safety margins and reasonable upper bounds
- Located in src/backend/utils/misc/guc.c:1591-1645