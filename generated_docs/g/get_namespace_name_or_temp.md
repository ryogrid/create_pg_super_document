# get_namespace_name_or_temp

## Location
src/backend/utils/cache/lsyscache.c: 3390 - 3406

## Overview
Returns the name of a PostgreSQL namespace, but returns "pg_temp" if it is the current backend's temporary namespace.

## Definition
```c
char *get_namespace_name_or_temp(Oid nspid)
```

## Detailed Description
The get_namespace_name_or_temp function is a specialized version of get_namespace_name that provides special handling for temporary namespaces. When the provided namespace OID corresponds to the current backend's temporary namespace, it returns the standardized string "pg_temp" instead of the actual internal temporary namespace name. For all other namespaces, it delegates to get_namespace_name to return the actual namespace name. This function is commonly used in contexts where a consistent, user-friendly representation of temporary namespaces is desired.

## Parameters / Member Variables
- `nspid`: The OID (Object Identifier) of the namespace whose name is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - isTempNamespace (checks if namespace is current backend's temp namespace)
  - pstrdup (string duplication with palloc)
  - get_namespace_name (retrieves actual namespace name)
- Called from (representative examples):
  - getObjectIdentityParts
  - getOpFamilyIdentity
  - getRelationIdentity
  - pg_event_trigger_ddl_commands
  - ExplainTargetRel
  - format_type_extended
  - format_procedure_parts
  - format_operator_parts
  - pg_get_functiondef
  - generate_relation_name
  - generate_qualified_relation_name
  - generate_function_name
  - generate_operator_name
  - generate_qualified_type_name

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Provides a user-friendly representation of temporary namespaces as "pg_temp"
- Used extensively in object identity and description functions where consistent naming is important
- Helps hide the internal implementation details of temporary namespace naming from users
- Part of the PG_NAMESPACE CACHE section in lsyscache.c
- Essential for DDL command logging, EXPLAIN output, and rule/view definition formatting where temporary objects need consistent representation