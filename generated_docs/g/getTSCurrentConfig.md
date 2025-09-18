# getTSCurrentConfig

## Location
[src/backend/utils/cache/ts_cache.c:556-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L556-L601)

## Overview
Returns the object identifier (OID) for the current text search configuration, with caching support and optional error handling.

## Definition


## Detailed Description
This function retrieves the OID of the currently configured text search configuration. It first checks a cache (TSCurrentConfigCache) for a previously resolved value to avoid repeated lookups. If no cached value exists, it parses the TSCurrentConfig GUC variable to resolve the configuration name to its corresponding OID. The function supports both error-throwing and error-suppressing modes based on the emitError parameter. When emitError is false, it uses an ErrorSaveContext to handle parsing errors gracefully.

## Parameters / Member Variables
- `emitError`: Boolean flag that controls error handling behavior. When true, errors are thrown; when false, errors are suppressed and InvalidOid is returned

## Dependencies
- Functions called/Symbols referenced:
  - [init_ts_config_cache](../i/init_ts_config_cache.md)
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md)  
  - [get_ts_config_oid](get_ts_config_oid.md)
  - [ErrorSaveContext](../E/ErrorSaveContext.md)
- Called from (representative examples):
  - [get_current_ts_config](get_current_ts_config.md)
  - [to_tsvector](../t/to_tsvector.md)
  - [to_tsquery](../t/to_tsquery.md)
  - [ts_headline](../t/ts_headline.md) functions

## Notes and Other Information
- Uses caching mechanism (TSCurrentConfigCache) to improve performance on repeated calls
- Initializes the text search configuration cache on first use via init_ts_config_cache()
- Handles both qualified and unqualified configuration names through stringToQualifiedNameList
- Part of PostgreSQL's text search functionality for full-text search operations
- The function is thread-safe as it operates on global state that's properly managed by the GUC system