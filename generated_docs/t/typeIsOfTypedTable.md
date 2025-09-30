# typeIsOfTypedTable

## Location
[src/backend/parser/parse_coerce.c:3382-3404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L3382-L3404)

## Overview
Checks whether a given relation type ID corresponds to the row type of a typed table with a specific type, or is a domain over such a row type.

## Definition

```c
static bool
typeIsOfTypedTable(Oid reltypeId, Oid reloftypeId)
```
## Detailed Description
This function determines if  represents the row type of a typed table that is of type , or if it's a domain over such a row type. The function is conceptually similar to the subtype relationship checking performed by .

The function works by:
1. First resolving the relation ID from the type using  to handle both direct relation types and domains over relation types
2. If a valid relation ID is found, it looks up the relation's catalog entry in 
3. Checks if the relation's  field matches the specified 
4. Returns true if there's a match, false otherwise

This is primarily used in type coercion scenarios where PostgreSQL needs to determine if one type can be safely converted to another based on typed table relationships.

## Parameters / Member Variables
- : The OID of the relation type to check
- : The OID of the target typed table type to match against

## Dependencies
- Functions called/Symbols referenced:
  - : Resolves the relation ID from a type, handling both direct relation types and domains
  - : Structure representing a row in the pg_class system catalog
  - : System cache lookup function
  - : Validates heap tuple
  - : Macro to extract structure from heap tuple
  - : Releases system cache reference
- Called from:
  - : Type coercion function at src/backend/parser/parse_coerce.c:509
  - : Type coercion capability checker at src/backend/parser/parse_coerce.c:638

## Notes and Other Information
- This is a static function within the parse_coerce.c module, so it's only accessible within that compilation unit
- The function properly handles error cases by using  when system cache lookups fail
- It follows PostgreSQL's pattern of using system cache lookups for efficient access to catalog information
- The function is part of PostgreSQL's type coercion infrastructure, which is critical for SQL type compatibility and conversions
- Typed tables are a PostgreSQL feature that allows creating tables based on user-defined composite types

## Simplified Source

```c
static bool
typeIsOfTypedTable(Oid reltypeId, Oid reloftypeId)
{
    Oid relid = typeOrDomainTypeRelid(reltypeId);
    bool result = false;

    if (relid)
    {
        // Look up the relation in pg_class
        HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

        if (!HeapTupleIsValid(tp))
            elog(ERROR, "cache lookup failed for relation %u", relid);

        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

        // Check if relation is typed with the specified type
        if (reltup->reloftype == reloftypeId)
            result = true;

        ReleaseSysCache(tp);
    }

    return result;
}
```