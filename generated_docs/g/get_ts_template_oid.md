# get_ts_template_oid

## Location
[src/backend/catalog/namespace.c:3007-3064](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3007-L3064)

## Overview
get_ts_template_oid finds and returns the OID of a text search template by its qualified or unqualified name, with optional error handling for missing templates.

## Definition


## Detailed Description
This function resolves a text search template name to its corresponding OID in the PostgreSQL system catalogs. It supports both qualified names (schema.template) and unqualified names (template only). For unqualified names, it searches through the current namespace search path to find the first matching template. The function implements standard PostgreSQL namespace resolution semantics and provides flexible error handling through the missing_ok parameter.

The lookup process follows these steps:
1. Parse the provided name list to extract schema and template components
2. If a schema is specified, perform direct lookup in that specific namespace
3. If no schema is specified, search through the active search path namespaces in order
4. Return the first matching template OID found, or handle missing templates based on the missing_ok flag

## Parameters / Member Variables
- : A list of strings representing the qualified or unqualified template name (e.g., {'myschema', 'mytemplate'} or just {'mytemplate'})
- : If true, returns InvalidOid when template is not found; if false, throws an error for missing templates

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md) (to parse qualified names)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md) (to find specific schema)
  - GetSysCacheOid2 (to lookup template in pg_ts_template catalog)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (to ensure current search path)
  - [NameListToString](../N/NameListToString.md) (for error message formatting)
  - ereport (for error reporting)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (for object address resolution)
  - [DefineTSDictionary](../D/DefineTSDictionary.md) (when creating text search dictionaries)
  - Various other text search management functions

## Notes and Other Information
- Returns InvalidOid for non-existent templates when missing_ok is true
- Temporary namespaces are explicitly skipped during search path traversal
- Follows PostgreSQL's standard qualified name resolution patterns used throughout the system
- Essential for text search template management and dictionary creation processes
- The function ensures proper namespace isolation and search path semantics for text search objects