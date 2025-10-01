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
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup for type information)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md) (namespace name resolution) 
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md) (proper quoting of schema.typename format)
- Called from (representative examples):
  - [pg_get_constraintdef_worker](../p/pg_get_constraintdef_worker.md) (constraint definition formatting, likely for domain constraints)

## Notes and Other Information
- Always schema-qualifies type names, unlike format_type_be()
- No special handling for SQL-standard type names
- Primarily used for domains in current PostgreSQL usage
- Returns allocated memory that caller must manage
- Essential for generating unambiguous type references in constraint definitions
- Part of the rule/constraint decompilation system

## Simplified Source

```c
// Simplified version of generate_qualified_type_name
static char *generate_qualified_type_name(Oid typid) {
    HeapTuple tp;
    Form_pg_type typtup;
    char *typname;
    char *nspname;
    char *result;

    // Look up type information in system cache
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for type %u", typid);

    // Extract type name from the tuple
    typtup = (Form_pg_type) GETSTRUCT(tp);
    typname = NameStr(typtup->typname);

    // Get the namespace name for schema qualification
    nspname = get_namespace_name_or_temp(typtup->typnamespace);
    if (!nspname)
        elog(ERROR, "cache lookup failed for namespace %u", typtup->typnamespace);

    // Create fully qualified name: schema.typename
    result = quote_qualified_identifier(nspname, typname);

    ReleaseSysCache(tp);
    return result;
}
```

Key simplifications made:
- Removed detailed comments while preserving error handling logic
- Added clear high-level comments explaining each major step
- Focused on the core algorithm: lookup type → extract names → qualify → return
- Preserved essential error checking for cache lookup failures
- Maintained the important schema qualification behavior