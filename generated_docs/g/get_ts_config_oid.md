# get_ts_config_oid

## Location
[src/backend/catalog/namespace.c:3152-3209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3152-L3209)

## Overview
get_ts_config_oid finds and returns the OID of a text search configuration by its qualified or unqualified name, with optional error handling for missing configurations.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &config_name);
```
## Detailed Description
This function resolves a text search configuration name to its corresponding OID in the PostgreSQL system catalogs. It supports both qualified names (schema.config) and unqualified names (config only). For unqualified names, it searches through the current namespace search path to find the first matching configuration. The function implements standard PostgreSQL namespace resolution semantics and provides flexible error handling through the missing_ok parameter.

The lookup process follows these steps:
1. Parse the provided name list to extract schema and configuration components
2. If a schema is specified, perform direct lookup in that specific namespace
3. If no schema is specified, search through the active search path namespaces in order
4. Return the first matching configuration OID found, or handle missing configurations based on the missing_ok flag

This function is essential for text search operations as configurations define how text is processed, tokenized, and indexed.

## Parameters / Member Variables
- : A list of strings representing the qualified or unqualified configuration name (e.g., {'myschema', 'myconfig'} or just {'myconfig'})
- : If true, returns InvalidOid when configuration is not found; if false, throws an error for missing configurations

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md) (to parse qualified names)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md) (to find specific schema)
  - GetSysCacheOid2 (to lookup configuration in pg_ts_config catalog)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (to ensure current search path)
  - [NameListToString](../N/NameListToString.md) (for error message formatting)
  - ereport (for error reporting)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (for object address resolution)
  - [GetTSConfigTuple](../G/GetTSConfigTuple.md) (for configuration tuple retrieval)
  - [DefineTSConfiguration](../D/DefineTSConfiguration.md) (when creating text search configurations)
  - [regconfigin](../r/regconfigin.md) (for regconfig type input)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md) (for automatic tsvector updates)
  - [getTSCurrentConfig](getTSCurrentConfig.md) (for default configuration resolution)
  - [check_default_text_search_config](../c/check_default_text_search_config.md) (for configuration validation)

## Notes and Other Information
- Returns InvalidOid for non-existent configurations when missing_ok is true
- Temporary namespaces are explicitly skipped during search path traversal
- Follows PostgreSQL's standard qualified name resolution patterns used throughout the system
- Critical for text search functionality as configurations control parsing, stemming, and indexing behavior
- Used extensively in full-text search operations and configuration management
- The function ensures proper namespace isolation and search path semantics for text search objects