# load_domaintype_info

## Location
src/backend/utils/cache/typcache.c: 994 - 1229

## Overview
A helper function that loads and caches domain constraint information for PostgreSQL domain types, including CHECK constraints and NOT NULL constraints from the type hierarchy.

## Definition
```c
static void load_domaintype_info(TypeCacheEntry *typentry)
```

## Detailed Description
This function is responsible for loading domain constraint information into the PostgreSQL type cache system. It scans the pg_constraint system catalog to find all constraints that apply to a domain type, including constraints inherited from parent domains in the type hierarchy.

The function performs several key operations:
1. Releases any existing stale constraint information
2. Crawls up the domain type hierarchy to collect constraints from all ancestor domains
3. Processes CHECK constraints by parsing and planning the constraint expressions
4. Handles NOT NULL constraints if specified in the domain definition
5. Creates a DomainConstraintCache structure in a dedicated memory context
6. Sorts constraints deterministically to ensure consistent application order
7. Attaches the constraint cache to the type cache entry

The function optimizes for the common case of no constraints by deferring memory allocation until constraints are actually found.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry that will be populated with domain constraint information. This entry represents the domain type being processed.

## Dependencies
- Functions called/Symbols referenced:
  - [decr_dcc_refcount](../d/decr_dcc_refcount.md)
  - table_open
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [fastgetattr](../f/fastgetattr.md)
  - TextDatumGetCString
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [stringToNode](../s/stringToNode.md)
  - [expression_planner](../e/expression_planner.md)
  - makeNode
  - qsort
  - [dcs_cmp](../d/dcs_cmp.md)
  - [lcons](lcons.md)
  - MemoryContextSetParent
- Called from (representative examples):
  - [lookup_type_cache](lookup_type_cache.md)
  - [UpdateDomainConstraintRef](../U/UpdateDomainConstraintRef.md)

## Notes and Other Information
- This is a static function, only accessible within typcache.c
- Uses a dedicated memory context ('Domain constraints') for constraint data to enable proper cleanup
- Implements reference counting for DomainConstraintCache objects to support sharing
- Constraint expressions are pre-planned for efficiency during runtime evaluation
- Constraints from parent domains are applied before child domain constraints (using lcons)
- The function assumes it's called in a short-lived context and may leak temporary data
- Sets the TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS flag to mark the type cache entry as having valid domain data
- Handles both CHECK constraints (with expressions) and NOT NULL constraints (boolean flags)