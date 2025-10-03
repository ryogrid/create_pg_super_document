# lookup_rowtype_tupdesc_domain

## Location
[src/backend/utils/cache/typcache.c:1889-1925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1889-L1925)

## Overview
Looks up a TupleDesc for a row type, with special handling for domains over composite types, providing a faster alternative to calling getBaseType() followed by lookup_rowtype_tupdesc_noerror().

## Definition

```c
TupleDesc
lookup_rowtype_tupdesc_domain(Oid type_id, int32 typmod, bool noError)
```
## Detailed Description
This function extends the functionality of lookup_rowtype_tupdesc_noerror() by handling domains over named composite types transparently. When the input type is a domain, it automatically resolves to the base composite type and retrieves its TupleDesc. This optimization avoids the need for callers to explicitly call getBaseType() before looking up the tuple descriptor.

The function serves a critical role in PostgreSQL's type system by bridging domain types and their underlying composite structures. However, it intentionally keeps callers aware they might be dealing with a domain type, ensuring proper domain constraint handling when constructing tuples.

For RECORD types, it delegates to lookup_rowtype_tupdesc_internal(), while for other types it uses the type cache system to efficiently resolve both regular composite types and domain-wrapped composite types.

## Parameters / Member Variables
- `type_id`: OID of the type to look up (can be a composite type, domain over composite type, or RECORDOID)
- `typmod`: Type modifier value that may affect the specific variant of the type
- `noError`: If true, returns NULL on failure instead of throwing an error
## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](lookup_type_cache.md)
  - [lookup_rowtype_tupdesc_noerror](lookup_rowtype_tupdesc_noerror.md)  
  - [lookup_rowtype_tupdesc_internal](lookup_rowtype_tupdesc_internal.md)
  - PinTupleDesc
- Called from (representative examples):
  - [ExecEvalWholeRowVar](../E/ExecEvalWholeRowVar.md) (src/backend/executor/execExprInterp.c:4843)
  - [rowtype_field_matches](../r/rowtype_field_matches.md) (src/backend/optimizer/util/clauses.c:2196)
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md) (src/pl/plperl/plperl.c:1378)

## Notes and Other Information
- The function automatically pins the returned TupleDesc using PinTupleDesc() to prevent premature deallocation
- Unlike plain lookup_rowtype_tupdesc(), this variant intentionally exposes domain handling to callers
- Efficient caching is achieved through the type cache system with TYPECACHE_TUPDESC and TYPECACHE_DOMAIN_BASE_INFO flags
- Returns NULL when noError=true and the type is not composite, otherwise throws ERRCODE_WRONG_OBJECT_TYPE error

## Simplified Source

```c
TupleDesc
lookup_rowtype_tupdesc_domain(Oid type_id, int32 typmod, bool noError)
{
    TupleDesc tupDesc;

    if (type_id != RECORDOID)
    {
        // Load type cache entry with tuple descriptor and domain info
        TypeCacheEntry *typentry = lookup_type_cache(type_id,
                                                     TYPECACHE_TUPDESC |
                                                     TYPECACHE_DOMAIN_BASE_INFO);

        // If this is a domain, recurse with the base type
        if (typentry->typtype == TYPTYPE_DOMAIN)
            return lookup_rowtype_tupdesc_noerror(typentry->domainBaseType,
                                                 typentry->domainBaseTypmod,
                                                 noError);

        // Check if we have a valid composite type
        if (typentry->tupDesc == NULL && !noError)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("type %s is not composite",
                            format_type_be(type_id))));

        tupDesc = typentry->tupDesc;
    }
    else
    {
        // Handle RECORD types
        tupDesc = lookup_rowtype_tupdesc_internal(type_id, typmod, noError);
    }

    // Pin the tuple descriptor to prevent deallocation
    if (tupDesc != NULL)
        PinTupleDesc(tupDesc);

    return tupDesc;
}
```