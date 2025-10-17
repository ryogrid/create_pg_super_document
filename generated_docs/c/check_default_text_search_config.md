# check_default_text_search_config

## Location
[src/backend/utils/cache/ts_cache.c:602-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L602-L669)

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
  - [IsTransactionState](../I/IsTransactionState.md)
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md)
  - [get_ts_config_oid](../g/get_ts_config_oid.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [guc_free](../g/guc_free.md)
  - [guc_strdup](../g/guc_strdup.md)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- Only performs catalog validation when inside a transaction with a valid database connection
- Uses ErrorSaveContext for graceful error handling during name parsing
- For PGC_S_TEST source, issues NOTICE instead of hard error for non-existent configurations
- Modifies the stored value to be fully qualified to prevent search_path dependency issues
- Uses GUC memory management functions (guc_free, guc_strdup) for proper memory handling
- Part of PostgreSQL's GUC (Grand Unified Configuration) system infrastructure

## Simplified Source

```c
bool check_default_text_search_config(char **newval, void **extra, GucSource source)
{
    // Skip validation if not in transaction or no database connection
    if (IsTransactionState() && MyDatabaseId != InvalidOid) {
        ErrorSaveContext escontext = {T_ErrorSaveContext};
        List *namelist;
        Oid cfgId;

        // Parse configuration name
        namelist = stringToQualifiedNameList(*newval, (Node *) &escontext);
        if (namelist != NIL)
            cfgId = get_ts_config_oid(namelist, true);
        else
            cfgId = InvalidOid;  // bad syntax

        // Handle non-existent configuration
        if (!OidIsValid(cfgId)) {
            if (source == PGC_S_TEST) {
                // Just issue notice for test mode
                ereport(NOTICE,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("text search configuration \"%s\" does not exist", *newval)));
                return true;
            } else {
                return false;  // reject invalid config
            }
        }

        // Normalize to fully qualified name to avoid search_path issues
        HeapTuple tuple = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgId));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for text search configuration %u", cfgId);

        Form_pg_ts_config cfg = (Form_pg_ts_config) GETSTRUCT(tuple);
        char *buf = quote_qualified_identifier(get_namespace_name(cfg->cfgnamespace), NameStr(cfg->cfgname));
        ReleaseSysCache(tuple);

        // Replace value with fully qualified name using GUC memory functions
        guc_free(*newval);
        *newval = guc_strdup(LOG, buf);
        pfree(buf);

        if (!*newval)
            return false;
    }

    return true;
}
```