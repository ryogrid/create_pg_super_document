# getTSCurrentConfig

## Location
src/backend/utils/cache/ts_cache.c: 556 - 601

## Overview
Returns the object identifier (OID) for the current text search configuration, with caching support and optional error handling.

## Definition


## Detailed Description
This function retrieves the OID of the currently configured text search configuration. It first checks a cache (TSCurrentConfigCache) for a previously resolved value to avoid repeated lookups. If no cached value exists, it parses the TSCurrentConfig GUC variable to resolve the configuration name to its corresponding OID. The function supports both error-throwing and error-suppressing modes based on the emitError parameter. When emitError is false, it uses an ErrorSaveContext to handle parsing errors gracefully.

## Parameters / Member Variables
- `emitError`: Boolean flag that controls error handling behavior. When true, errors are thrown; when false, errors are suppressed and InvalidOid is returned

## Dependencies
- Functions called/Symbols referenced:
  - init_ts_config_cache
  - stringToQualifiedNameList  
  - get_ts_config_oid
  - ErrorSaveContext
- Called from (representative examples):
  - get_current_ts_config
  - to_tsvector
  - to_tsquery
  - ts_headline functions

## Notes and Other Information
- Uses caching mechanism (TSCurrentConfigCache) to improve performance on repeated calls
- Initializes the text search configuration cache on first use via init_ts_config_cache()
- Handles both qualified and unqualified configuration names through stringToQualifiedNameList
- Part of PostgreSQL's text search functionality for full-text search operations
- The function is thread-safe as it operates on global state that's properly managed by the GUC system