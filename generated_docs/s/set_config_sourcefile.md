# set_config_sourcefile

## Location
[src/backend/utils/misc/guc.c:4302-4334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4302-L4334)

## Overview
Sets the source file and line number information for a configuration parameter to track where the setting originated.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This internal function updates the source file and line number metadata for a configuration parameter. It's used to track the origin of configuration settings, particularly when reading from configuration files. The function helps with debugging and auditing by maintaining information about where each configuration value was set. It handles memory management by duplicating the source file string and freeing any previously stored source file information.

This function is part of the internal GUC system infrastructure and is typically called during configuration file processing to maintain provenance information for each parameter setting.

## Parameters / Member Variables
- : Name of the configuration parameter to update
- : Path to the source file where the setting was defined
- : Line number in the source file where the setting was defined

## Dependencies
- Functions called/Symbols referenced:
  - find_option
  - [guc_strdup](../g/guc_strdup.md)
  - [guc_free](../g/guc_free.md)
  - config_generic
  - DEBUG3, LOG constants
- Called from (representative examples):
  - [define_custom_variable](../d/define_custom_variable.md)
  - [read_nondefault_variables](../r/read_nondefault_variables.md)
  - RestoreGUCState
  - GUCHashEntry

## Notes and Other Information
- This is a static (internal) function within the GUC module
- Handles memory management by duplicating source file strings
- Uses different error levels depending on whether running under postmaster
- Source file information is primarily used for debugging and configuration tracking
- Located in src/backend/utils/misc/guc.c:4302-4334