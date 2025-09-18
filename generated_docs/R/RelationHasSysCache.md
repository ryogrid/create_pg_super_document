# RelationHasSysCache

## Location
src/backend/utils/cache/syscache.c: 746 - 770

## Overview
Tests whether a given relation has an associated system catalog cache by performing a binary search on the sorted list of cached relation OIDs.

## Definition


## Detailed Description
RelationHasSysCache efficiently determines if a relation is backed by a system catalog cache using a binary search algorithm. The function searches through the SysCacheRelationOid array, which contains a sorted list of all relation OIDs that have associated system caches. This lookup is used by various parts of PostgreSQL to determine the appropriate invalidation and caching behavior for different system catalogs.

## Parameters / Member Variables
- `relid`: Object identifier (Oid) of the relation to check for system cache presence

## Dependencies
- Functions called/Symbols referenced:
  - Uses SysCacheRelationOid array and SysCacheRelationOidSize for binary search
- Called from (representative examples):
  - [GetNonHistoricCatalogSnapshot](../G/GetNonHistoricCatalogSnapshot.md)
  - Referenced in syscache.h header

## Notes and Other Information
- Implements an efficient O(log n) binary search algorithm for cache lookup
- Relies on SysCacheRelationOid being pre-sorted for correct binary search operation
- Used primarily by snapshot management and invalidation systems
- Complements RelationInvalidatesSnapshotsOnly() for determining relation caching behavior
- Located in src/backend/utils/cache/syscache.c:746-770