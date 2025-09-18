# check_default_text_search_config

## Location
src/backend/utils/cache/ts_cache.c: 602 - 669

## Overview
A GUC check hook function that validates and normalizes text search configuration names when the default_text_search_config parameter is set.

## Definition
bool check_default_text_search_config(char **newval, void **extra, GucSource source)

## Detailed Description
This function serves as a validation hook for the default_text_search_config GUC parameter. It validates that the specified text search configuration exists in the system catalogs and normalizes the configuration name to be fully qualified (schema.name format). The function handles different validation modes based on the GucSource - for test sources, it only issues a NOTICE for non-existent configurations rather than rejecting the value. When inside a transaction with a valid database connection, it performs catalog lookups to verify the configuration exists and converts the name to a fully qualified form to ensure search_path changes don't affect the setting.

## Parameters / Member Variables
- `newval`: Pointer to the new configuration value string that will be validated and potentially modified
- `extra`: Pointer for storing extra data (unused in this function)  
- `source`: The source of the GUC setting change, affects validation behavior

## Dependencies
- Functions called/Symbols referenced:
  - IsTransactionState
  - stringToQualifiedNameList
  - get_ts_config_oid
  - SearchSysCache1
  - quote_qualified_identifier
  - get_namespace_name
  - guc_free
  - guc_strdup
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- Only performs catalog validation when inside a transaction with a valid database connection
- Uses ErrorSaveContext for graceful error handling during name parsing
- For PGC_S_TEST source, issues NOTICE instead of hard error for non-existent configurations
- Modifies the stored value to be fully qualified to prevent search_path dependency issues
- Uses GUC memory management functions (guc_free, guc_strdup) for proper memory handling
- Part of PostgreSQL's GUC (Grand Unified Configuration) system infrastructure