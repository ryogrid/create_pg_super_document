# generate_qualified_type_name

## Location
[src/backend/utils/adt/ruleutils.c:13180-13212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13180-L13212)

## Overview
Computes the name to display for a type specified by OID, always using schema-qualified naming.

## Definition
```c
static char *generate_qualified_type_name(Oid typid)
```

## Detailed Description
This function generates a fully-qualified type name (schema.typename) for a given type OID. Unlike `format_type_be()`, this function unconditionally schema-qualifies the name, ensuring that the type can be unambiguously referenced regardless of the current search path settings.

The function does not provide special syntax for SQL-standard type names, making it different from the more general type formatting functions. The current usage context suggests this function is primarily used for domains, where such special syntax cases would not occur.

The function returns a newly allocated string containing the properly quoted and qualified type name.

## Parameters / Member Variables
- `typid`: OID of the type to generate a qualified name for

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup for type information)
  - get_namespace_name_or_temp (namespace name resolution) 
  - quote_qualified_identifier (proper quoting of schema.typename format)
- Called from (representative examples):
  - pg_get_constraintdef_worker (constraint definition formatting, likely for domain constraints)

## Notes and Other Information
- Always schema-qualifies type names, unlike format_type_be()
- No special handling for SQL-standard type names
- Primarily used for domains in current PostgreSQL usage
- Returns allocated memory that caller must manage
- Essential for generating unambiguous type references in constraint definitions
- Part of the rule/constraint decompilation system