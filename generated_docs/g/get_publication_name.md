# get_publication_name

## Location
[src/backend/utils/cache/lsyscache.c:3645-3674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3645-L3674)

## Overview
Retrieves the name of a publication given its object identifier (OID), with optional error handling for missing publications.

## Definition
```c
char *get_publication_name(Oid pubid, bool missing_ok)
```

## Detailed Description
This function performs a reverse lookup in the PostgreSQL system cache to find the name of a publication identified by its OID. Publications are central to PostgreSQL's logical replication system, and this function is commonly used when displaying publication information or generating error messages that need to include publication names.

The function searches the PUBLICATIONOID system cache to retrieve the publication's metadata, extracts the publication name from the Form_pg_publication structure, and returns a dynamically allocated copy of the name string. The behavior when a publication is not found depends on the missing_ok parameter.

## Parameters / Member Variables
- `pubid`: The object identifier (OID) of the publication to look up
- `missing_ok`: If false, throw an error when publication is not found; if true, return NULL instead

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_publication
- Called from (representative examples):
  - [getPublicationSchemaInfo](getPublicationSchemaInfo.md)
  - [getObjectDescription](getObjectDescription.md)
  - [getObjectIdentityParts](getObjectIdentityParts.md)

## Notes and Other Information
- Returns a palloc'd string that should be freed by the caller when no longer needed
- This function is the reverse operation of get_publication_oid
- Used extensively in object description and identity functions for system catalogs
- Part of the logical replication infrastructure in PostgreSQL
- Located in src/backend/utils/cache/lsyscache.c:3645-3674
- The returned string is a copy, so modifications won't affect the system catalog

## Simplified Source

```c
char *
get_publication_name(Oid pubid, bool missing_ok)
{
    HeapTuple tup;
    char *pubname;
    Form_pg_publication pubform;

    // Look up publication by OID
    tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

    if (!HeapTupleIsValid(tup)) {
        if (!missing_ok)
            elog(ERROR, "cache lookup failed for publication %u", pubid);
        return NULL;
    }

    // Extract and copy publication name
    pubform = (Form_pg_publication) GETSTRUCT(tup);
    pubname = pstrdup(NameStr(pubform->pubname));

    ReleaseSysCache(tup);
    return pubname;
}
```