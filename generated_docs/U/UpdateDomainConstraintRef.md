# UpdateDomainConstraintRef

## Location
[src/backend/utils/cache/typcache.c:1351-1399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1351-L1399)

## Overview
Rechecks the validity of domain constraint information and updates the reference to point at current cached constraints if the domain's constraint set has changed.

## Definition

```c
void
UpdateDomainConstraintRef(DomainConstraintRef *ref)
```
## Detailed Description
This function performs validation and synchronization of domain constraint references with the current state of the domain's constraint information. It is designed to be called before each use of constraint information to ensure consistency. The function first ensures the type cache entry is up to date by loading domain information if necessary, then compares the current constraint cache with the referenced one.

When constraint changes are detected, the function performs proper reference count management by decrementing the old constraint cache reference count and incrementing the new one. If expression states are needed, it calls prep_domain_constraints to prepare executable constraint states. The function includes a deliberate design decision to leak previous executable domain constraint lists rather than managing child memory contexts, as constraint updates are expected to be rare.

## Parameters / Member Variables
- : Pointer to the DomainConstraintRef structure to be updated with current constraint information

## Dependencies
- Functions called/Symbols referenced:
  - [DomainConstraintRef](../D/DomainConstraintRef.md) (struct for managing constraint references)  
  - TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS (flag indicating domain constraints have been checked)
  - TYPTYPE_DOMAIN (type classification for domain types)
  - [load_domaintype_info](../l/load_domaintype_info.md) (loads domain type constraint information)
  - [DomainConstraintCache](../D/DomainConstraintCache.md) (struct for cached domain constraint data)
  - [decr_dcc_refcount](../d/decr_dcc_refcount.md) (decrements domain constraint cache reference count)
  - [prep_domain_constraints](../p/prep_domain_constraints.md) (prepares constraints for execution)
- Called from (representative examples):
  - [domain_check_input](../d/domain_check_input.md) (domain input validation)

## Notes and Other Information
- This function is expected to be called before each use of constraint information, making performance critical
- The function deliberately leaks previous executable constraint lists to avoid overhead of managing child memory contexts
- Reference counting ensures proper cleanup of shared constraint cache data
- The function only loads domain type info when the TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS flag indicates it's needed
- Memory leakage is considered acceptable due to the rarity of constraint updates in practice