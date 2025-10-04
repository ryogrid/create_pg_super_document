# refnameNamespaceItem

## Location
[src/backend/parser/parse_relation.c:129-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L129-L199)

## Overview
Searches for a namespace item (table, view, CTE, etc.) that matches a given reference name, supporting both qualified and unqualified names across nested parsing contexts.

## Definition

```c
ParseNamespaceItem *
refnameNamespaceItem(ParseState *pstate,
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up)
```
## Detailed Description
This function performs namespace resolution for relation references in SQL queries. It handles both qualified names (schema.table) and unqualified names (table), searching through the parsing state's namespace stack to find matching items. For qualified names, it converts the schema.relation pair to a relation OID and searches by relid. For unqualified names, it searches by alias or relation name. The function can optionally track nesting depth and will traverse parent parsing states when sublevels_up is provided.

## Parameters / Member Variables
- `*pstate`: Current parsing state containing the namespace stack
- `*schemaname`: Schema name for qualified references (NULL for unqualified)
- `*refname`: The relation/alias name to search for
- `location`: Source location for error reporting
- `*sublevels_up`: Optional output parameter for nesting depth (NULL to search current level only)
## Dependencies
- Functions called/Symbols referenced:
  - [LookupNamespaceNoError](../L/LookupNamespaceNoError.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - [scanNameSpaceForRelid](../s/scanNameSpaceForRelid.md)
  - [scanNameSpaceForRefname](../s/scanNameSpaceForRefname.md)
- Called from (representative examples):
  - [errorMissingRTE](../e/errorMissingRTE.md)
  - Various parser error handling functions in parse_expr.c and parse_target.c

## Notes and Other Information
- For qualified names, the function performs OID-based lookup rather than name-based to match SQL semantics
- Returns NULL if no matching namespace item is found
- Can report ambiguity errors if multiple items match an unqualified name at the same nesting level
- Part of PostgreSQL's parser namespace resolution system

## Simplified Source

```c
ParseNamespaceItem *
refnameNamespaceItem(ParseState *pstate,
                     const char *schemaname,
                     const char *refname,
                     int location,
                     int *sublevels_up)
{
    Oid relId = InvalidOid;

    // Initialize sublevel tracking
    if (sublevels_up)
        *sublevels_up = 0;

    // Handle qualified names: convert schema.relation to OID
    if (schemaname != NULL)
    {
        Oid namespaceId = LookupNamespaceNoError(schemaname);
        if (!OidIsValid(namespaceId))
            return NULL;
        relId = get_relname_relid(refname, namespaceId);
        if (!OidIsValid(relId))
            return NULL;
    }

    // Search through parsing state stack
    while (pstate != NULL)
    {
        ParseNamespaceItem *result;

        // Search by OID for qualified names, by name for unqualified
        if (OidIsValid(relId))
            result = scanNameSpaceForRelid(pstate, relId, location);
        else
            result = scanNameSpaceForRefname(pstate, refname, location);

        if (result)
            return result;

        // Move to parent parsing state if requested
        if (sublevels_up)
            (*sublevels_up)++;
        else
            break;

        pstate = pstate->parentParseState;
    }
    return NULL;
}
```