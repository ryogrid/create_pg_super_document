# check_cluster_name

## Location
src/backend/commands/variable.c: 1106 - 1133

## Overview
A GUC (Grand Unified Configuration) check hook function that validates and sanitizes the `cluster_name` configuration parameter by ensuring it contains only clean ASCII characters.

## Definition
```c
bool check_cluster_name(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the `cluster_name` GUC parameter in PostgreSQL. When a user attempts to set or change the `cluster_name` configuration parameter, this hook function is automatically called by the GUC system to validate and potentially modify the input value before it is accepted.

The function performs ASCII character sanitization to ensure that the cluster name contains only printable, safe ASCII characters. This is important for security and compatibility reasons, as the cluster name may be displayed in various contexts including logs, monitoring systems, and administrative interfaces.

The function replaces the original input with a cleaned version, ensuring that any non-ASCII or potentially problematic characters are removed or replaced appropriately.

## Parameters / Member Variables
- `newval`: Pointer to the new value being assigned to the `cluster_name` parameter (can be modified by the function)
- `extra`: Pointer to additional data that can be passed to subsequent hook functions (currently unused)
- `source`: The source of the configuration change (e.g., configuration file, SQL command, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - `pg_clean_ascii`: Sanitizes the input string to contain only clean ASCII characters
  - `guc_strdup`: Duplicates a string using GUC memory management
  - `guc_free`: Frees memory allocated by GUC functions
  - `pfree`: PostgreSQL memory deallocation function
  - `GucSource`: Enumeration type indicating the source of configuration changes
  - `MCXT_ALLOC_NO_OOM`: Memory allocation flag to avoid out-of-memory errors
- Called from (representative examples):
  - GUC system infrastructure (referenced in `src/include/utils/guc_hooks.h`)

## Notes and Other Information
- This is part of PostgreSQL's GUC (Grand Unified Configuration) system
- The function modifies the input value in-place by replacing `*newval` with a sanitized version
- Returns `true` if the validation succeeds, `false` if it fails (e.g., due to memory allocation issues)
- The `cluster_name` parameter is typically used in high-availability and replication scenarios to identify different database clusters
- The ASCII sanitization helps prevent issues with character encoding and ensures consistent display across different systems and interfaces