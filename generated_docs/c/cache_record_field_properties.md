# cache_record_field_properties

## Location
[src/backend/utils/cache/typcache.c:1521-1625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1521-L1625)

## Overview
This function computes and caches the field properties for record types, determining which operations (equality, comparison, hashing, extended hashing) are supported by all fields of a composite type.

## Definition
static void cache_record_field_properties(TypeCacheEntry *typentry)

## Detailed Description
This function is the core implementation for determining what operations are supported by record types. It handles three main cases: RECORD pseudo-type (assumes equality and comparison), composite types (checks all fields), and domains over composite types (inherits base type properties). For composite types, it iterates through all non-dropped fields and checks if each field type supports the required operations. Only if ALL fields support an operation is that operation marked as available for the record type. The function uses reference counting for tuple descriptors to ensure safe access during catalog lookups.

## Parameters / Member Variables
- typentry: Pointer to a TypeCacheEntry structure that will be updated with computed field property flags indicating which operations are supported by the record type

## Dependencies
- Functions called/Symbols referenced:
  - [load_typcache_tupdesc](../l/load_typcache_tupdesc.md)
  - [IncrTupleDescRefCount](../I/IncrTupleDescRefCount.md)
  - [DecrTupleDescRefCount](../D/DecrTupleDescRefCount.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - TCFLAGS_HAVE_FIELD_EQUALITY (flag)
  - TCFLAGS_HAVE_FIELD_COMPARE (flag)
  - TCFLAGS_HAVE_FIELD_HASHING (flag)
  - TCFLAGS_HAVE_FIELD_EXTENDED_HASHING (flag)
  - TCFLAGS_CHECKED_FIELD_PROPERTIES (flag)
  - TCFLAGS_DOMAIN_BASE_IS_COMPOSITE (flag)
- Called from (representative examples):
  - [record_fields_have_equality](../r/record_fields_have_equality.md)
  - [record_fields_have_compare](../r/record_fields_have_compare.md)
  - [record_fields_have_hashing](../r/record_fields_have_hashing.md)
  - [record_fields_have_extended_hashing](../r/record_fields_have_extended_hashing.md)

## Notes and Other Information
- This is a static function only used within typcache.c
- Implements careful tuple descriptor reference counting to prevent crashes during catalog lookups
- For RECORD pseudo-type, conservatively assumes equality and comparison work but not hashing
- Uses early exit optimization - stops checking fields once all property flags are cleared
- For domain types over composite types, inherits properties from the base composite type
- Sets TCFLAGS_CHECKED_FIELD_PROPERTIES to prevent redundant computation
- Critical for PostgreSQL's type system to determine what operations can be performed on complex types

## Simplified Source

```c
static void cache_record_field_properties(TypeCacheEntry *typentry)
{
    // Handle RECORD pseudo-type: assume equality and comparison work
    if (typentry->type_id == RECORDOID)
    {
        typentry->flags |= (TCFLAGS_HAVE_FIELD_EQUALITY |
                           TCFLAGS_HAVE_FIELD_COMPARE);
    }
    // Handle composite types: check all fields for supported operations
    else if (typentry->typtype == TYPTYPE_COMPOSITE)
    {
        TupleDesc tupdesc;
        int newflags;

        // Load tuple descriptor if needed
        if (typentry->tupDesc == NULL)
            load_typcache_tupdesc(typentry);
        tupdesc = typentry->tupDesc;

        IncrTupleDescRefCount(tupdesc);

        // Start with all properties available, remove unsupported ones
        newflags = (TCFLAGS_HAVE_FIELD_EQUALITY |
                   TCFLAGS_HAVE_FIELD_COMPARE |
                   TCFLAGS_HAVE_FIELD_HASHING |
                   TCFLAGS_HAVE_FIELD_EXTENDED_HASHING);

        // Check each non-dropped field
        for (int i = 0; i < tupdesc->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

            if (attr->attisdropped)
                continue;

            // Get field type capabilities
            TypeCacheEntry *fieldentry = lookup_type_cache(attr->atttypid,
                TYPECACHE_EQ_OPR | TYPECACHE_CMP_PROC |
                TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC);

            // Remove unsupported operations
            if (!OidIsValid(fieldentry->eq_opr))
                newflags &= ~TCFLAGS_HAVE_FIELD_EQUALITY;
            if (!OidIsValid(fieldentry->cmp_proc))
                newflags &= ~TCFLAGS_HAVE_FIELD_COMPARE;
            if (!OidIsValid(fieldentry->hash_proc))
                newflags &= ~TCFLAGS_HAVE_FIELD_HASHING;
            if (!OidIsValid(fieldentry->hash_extended_proc))
                newflags &= ~TCFLAGS_HAVE_FIELD_EXTENDED_HASHING;

            // Early exit if no operations are supported
            if (newflags == 0)
                break;
        }

        typentry->flags |= newflags;
        DecrTupleDescRefCount(tupdesc);
    }
    // Handle domains over composite types: inherit base type properties
    else if (typentry->typtype == TYPTYPE_DOMAIN)
    {
        // Load base type info if needed
        if (typentry->domainBaseType == InvalidOid)
        {
            typentry->domainBaseTypmod = -1;
            typentry->domainBaseType = getBaseTypeAndTypmod(typentry->type_id,
                                                           &typentry->domainBaseTypmod);
        }

        TypeCacheEntry *baseentry = lookup_type_cache(typentry->domainBaseType,
            TYPECACHE_EQ_OPR | TYPECACHE_CMP_PROC |
            TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC);

        if (baseentry->typtype == TYPTYPE_COMPOSITE)
        {
            typentry->flags |= TCFLAGS_DOMAIN_BASE_IS_COMPOSITE;
            typentry->flags |= baseentry->flags & (TCFLAGS_HAVE_FIELD_EQUALITY |
                                                  TCFLAGS_HAVE_FIELD_COMPARE |
                                                  TCFLAGS_HAVE_FIELD_HASHING |
                                                  TCFLAGS_HAVE_FIELD_EXTENDED_HASHING);
        }
    }

    typentry->flags |= TCFLAGS_CHECKED_FIELD_PROPERTIES;
}
```