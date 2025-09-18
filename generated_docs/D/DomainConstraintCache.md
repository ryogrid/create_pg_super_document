# DomainConstraintCache

## Location
[src/backend/utils/cache/typcache.c:124-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L124-L135)

## Overview
DomainConstraintCache is a structure that stores cached information about domain type constraints to avoid repeatedly rebuilding constraint evaluation data.

## Definition
```c
struct DomainConstraintCache
{
    List       *constraints;    /* list of DomainConstraintState nodes */
    MemoryContext dccContext;   /* memory context holding all associated data */
    long        dccRefCount;    /* number of references to this struct */
};
```

## Detailed Description
The DomainConstraintCache struct is used to cache domain type constraint information in PostgreSQL type cache system. This cache stores expression plan trees for domain constraints but keeps the check_exprstate fields as NULL. When constraint evaluation is needed, expression evaluation nodes are built by flat-copying the DomainConstraintState nodes and applying ExecInitExpr to check_expr. This design allows for efficient reuse of constraint information while avoiding the overhead of maintaining active expression states when not needed.

The cache is only created when a domain type actually has constraints - for constraint-less domains, the domainData field is simply set to NULL. This optimization avoids unnecessary memory allocation for the common case of domains without constraints.

## Parameters / Member Variables
- `constraints`: A list of DomainConstraintState nodes representing the domain constraints
- `dccContext`: Memory context that holds all data associated with this constraint cache
- `dccRefCount`: Reference count tracking how many references exist to this cache structure

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL list structure)
  - [MemoryContext](../M/MemoryContext.md) (PostgreSQL memory management)
  - [DomainConstraintState](DomainConstraintState.md) (constraint state nodes)
- Called from (representative examples):
  - [load_domaintype_info](../l/load_domaintype_info.md)
  - [decr_dcc_refcount](../d/decr_dcc_refcount.md)
  - [dccref_deletion_callback](../d/dccref_deletion_callback.md)
  - [UpdateDomainConstraintRef](../U/UpdateDomainConstraintRef.md)

## Notes and Other Information
- The cache uses reference counting (dccRefCount) to manage memory lifecycle
- Expression evaluation nodes are created on-demand and belong to DomainConstraintRef rather than the cache itself
- This structure is part of PostgreSQL's type cache system located in src/backend/utils/cache/typcache.c
- The cache is designed to optimize domain constraint checking by avoiding repeated constraint parsing and planning