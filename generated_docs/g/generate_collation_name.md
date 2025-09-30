# generate_collation_name

## Location
[src/backend/utils/adt/ruleutils.c:13213-13244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13213-L13244)

## Overview
Computes the name to display for a collation specified by OID, with necessary quoting and schema-prefixing.

## Definition
```c
char *generate_collation_name(Oid collid)
```

## Detailed Description
This function generates a properly formatted collation name for display purposes. It handles schema qualification intelligently - the collation name is schema-qualified only if the collation is not visible in the current search path (as determined by `CollationIsVisible()`).

When schema qualification is needed, the function produces a fully-qualified name in the format `schema.collation`. When the collation is visible in the search path, only the collation name itself is returned. In both cases, proper identifier quoting is applied as needed.

The function returns a newly allocated string containing the appropriately formatted collation name.

## Parameters / Member Variables
- `collid`: OID of the collation to generate a name for

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup for collation information)
  - [CollationIsVisible](../C/CollationIsVisible.md) (visibility check based on search path)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md) (namespace name resolution when needed)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md) (proper quoting of names)
- Called from (representative examples):
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md) (index definition formatting)
  - [pg_get_partkeydef_worker](../p/pg_get_partkeydef_worker.md) (partition key definition formatting)
  - [get_rule_expr](get_rule_expr.md) (expression decompilation)
  - [get_const_collation](get_const_collation.md) (constant collation formatting)
  - [pg_collation_for](../p/pg_collation_for.md) (collation information function)

## Notes and Other Information
- Only schema-qualifies when collation is not visible in current search path
- Uses CollationIsVisible() to make intelligent qualification decisions
- Returns allocated memory that caller must manage
- Essential for proper collation specification in decompiled expressions and definitions
- Part of the rule decompilation system used for displaying stored database objects
- Public function (not static) indicating broader usage across the codebase

## Simplified Source

```c
char *generate_collation_name(Oid collid) {
    HeapTuple tp;
    Form_pg_collation colltup;
    char *collname;
    char *nspname;
    char *result;

    // Look up collation information in system cache
    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for collation %u", collid);

    // Extract collation name from tuple
    colltup = (Form_pg_collation) GETSTRUCT(tp);
    collname = NameStr(colltup->collname);

    // Determine if schema qualification is needed
    if (!CollationIsVisible(collid)) {
        // Not visible in search path - need schema qualification
        nspname = get_namespace_name_or_temp(colltup->collnamespace);
    } else {
        // Visible in search path - no schema needed
        nspname = NULL;
    }

    // Create properly quoted and qualified name
    result = quote_qualified_identifier(nspname, collname);

    // Clean up cache reference
    ReleaseSysCache(tp);

    return result;
}
```