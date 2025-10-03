# get_language_name

## Location
[src/bin/pg_dump/pg_dump.c:8691-8713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8691-L8713)

## Overview
Retrieves the name of a procedural language from the system cache given its OID, with optional error handling for missing languages.

## Definition

```c
static char *
get_language_name(Archive *fout, Oid langid)
```
## Detailed Description
The  function performs a cached lookup in the  system catalog to retrieve the name of a procedural language identified by its OID. It uses PostgreSQL's system cache mechanism for efficient access to frequently-needed language information. The function can optionally handle missing languages gracefully based on the  parameter.

When a language is found, the function extracts the language name from the  tuple and returns a palloc'd copy. If the language OID doesn't exist and  is false, it throws an ERROR. If  is true, it returns NULL for non-existent languages.

## Parameters / Member Variables
- `*fout`: The OID of the language to look up in the pg_language catalog
- `langid`: If true, return NULL for non-existent languages instead of throwing an error
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - Form_pg_language
  - GETSTRUCT
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - elog
- Called from (representative examples):
  - [getObjectDescription](getObjectDescription.md)
  - [getObjectIdentityParts](getObjectIdentityParts.md)
  - [get_transform_oid](get_transform_oid.md)
  - [pg_get_functiondef](../p/pg_get_functiondef.md)
  - [getTransforms](getTransforms.md)
  - [dumpTransform](../d/dumpTransform.md)

## Notes and Other Information
- Part of the language cache subsystem in lsyscache.c
- Uses the LANGOID cache for efficient lookup of language information
- Returns a palloc'd string that must be freed by the caller
- Commonly used in object description functions and pg_dump operations
- The returned string contains only the language name, not the full language definition

## Simplified Source

```c
char *
get_language_name(Oid langoid, bool missing_ok)
{
    HeapTuple tp;

    // Look up language by OID
    tp = SearchSysCache1(LANGOID, ObjectIdGetDatum(langoid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_language lantup = (Form_pg_language) GETSTRUCT(tp);
        char *result;

        // Extract and copy language name
        result = pstrdup(NameStr(lantup->lanname));
        ReleaseSysCache(tp);
        return result;
    }

    // Handle missing language
    if (!missing_ok)
        elog(ERROR, "cache lookup failed for language %u", langoid);
    return NULL;
}
```