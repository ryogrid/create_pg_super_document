# domain_state_setup

## Location
[src/backend/utils/adt/domains.c:76-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/domains.c#L76-L137)

## Overview
Initializes the cache for a new domain type, setting up I/O functions and constraint information.

## Definition

```c
static DomainIOData *
domain_state_setup(Oid domainType, bool binary, MemoryContext mcxt)
```
## Detailed Description
The  function creates and initializes a  structure that serves as a cache for domain type operations. This function performs validation of the domain type, sets up the appropriate I/O functions for the underlying base type, and initializes domain constraint checking infrastructure.

The function is designed to be used by domain input functions ( and ) to prepare the necessary context for processing domain values. It handles both text and binary input modes based on the  parameter.

An important note is that the cache struct cannot be reused for different domain types due to the lack of provision for releasing . If a call site needs to handle a new domain type, the old struct is leaked for the query duration.

## Parameters / Member Variables
- : OID of the domain type to set up cache for
- : Boolean flag indicating whether to set up for binary (true) or text (false) input
- : Memory context in which to allocate the cache structure

## Dependencies
- Functions called/Symbols referenced:
  - [DomainIOData](../D/DomainIOData.md) (struct type)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPECACHE_DOMAIN_BASE_INFO
  - TYPTYPE_DOMAIN
  - [getTypeBinaryInputInfo](../g/getTypeBinaryInputInfo.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [InitDomainConstraintRef](../I/InitDomainConstraintRef.md)

- Called from (representative examples):
  - [domain_in](domain_in.md) (src/backend/utils/adt/domains.c:256)
  - [domain_recv](domain_recv.md) (src/backend/utils/adt/domains.c:315)
  - [domain_check_internal](domain_check_internal.md) (src/backend/utils/adt/domains.c:389)

## Notes and Other Information
- The function validates that the provided OID represents a valid domain type using 
- Memory allocated for the cache structure is intentionally leaked when switching domain types within a query
- The function sets up both the underlying base type I/O functions and domain-specific constraint checking
- An ExprContext is not created until needed, optimizing memory usage
- The cache is marked as valid by setting the domain_type field to the input domainType

## Simplified Source

```c
static DomainIOData *domain_state_setup(Oid domainType, bool binary, MemoryContext mcxt) {
    DomainIOData *my_extra;
    TypeCacheEntry *typentry;
    Oid baseType;

    // Allocate cache structure
    my_extra = (DomainIOData *) MemoryContextAlloc(mcxt, sizeof(DomainIOData));

    // Validate domain type and get base type info
    typentry = lookup_type_cache(domainType, TYPECACHE_DOMAIN_BASE_INFO);
    if (typentry->typtype != TYPTYPE_DOMAIN) {
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("type %s is not a domain",
                        format_type_be(domainType))));
    }

    // Extract base type information
    baseType = typentry->domainBaseType;
    my_extra->typtypmod = typentry->domainBaseTypmod;

    // Setup I/O functions for base type
    if (binary) {
        getTypeBinaryInputInfo(baseType,
                              &my_extra->typiofunc,
                              &my_extra->typioparam);
    } else {
        getTypeInputInfo(baseType,
                        &my_extra->typiofunc,
                        &my_extra->typioparam);
    }
    fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc, mcxt);

    // Initialize domain constraints
    InitDomainConstraintRef(domainType, &my_extra->constraint_ref, mcxt, true);

    // Initialize remaining fields
    my_extra->econtext = NULL;  // Created when needed
    my_extra->mcxt = mcxt;
    my_extra->domain_type = domainType;  // Mark cache valid

    return my_extra;
}
``` 