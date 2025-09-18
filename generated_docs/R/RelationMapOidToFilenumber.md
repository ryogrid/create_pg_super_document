# RelationMapOidToFilenumber

## Location
src/backend/utils/cache/relmapper.c: 165 - 217

## Overview
Maps a relation OID to its corresponding file number by searching the relation mapping tables, supporting both shared and local relations.

## Definition
RelFileNumber RelationMapOidToFilenumber(Oid relationId, bool shared)

## Detailed Description
This is the primary function of the relation mapping system that performs the core OID-to-filenumber translation. The function searches through relation mapping tables to find the file number corresponding to a given relation OID. It supports both shared relations (visible across all databases) and local relations (specific to a database).

The function implements a two-tiered lookup strategy:
1. First checks active update maps (pending changes not yet committed)
2. Then falls back to the main mapping tables

This design ensures that uncommitted mapping changes are visible during transactions while maintaining consistency with the persistent mapping data.

## Parameters / Member Variables
- : The OID of the relation whose file number is being sought
- : Boolean flag indicating whether to search shared relation maps (true) or local relation maps (false)

## Dependencies
- Functions called/Symbols referenced:
  - [RelMapFile](RelMapFile.md) (structure type used for mapping tables)
  - InvalidRelFileNumber (returned when no mapping is found)
- Called from (representative examples):
  - [swap_relation_files](../s/swap_relation_files.md) (cluster.c:1166, 1170)
  - [pg_relation_filenode](../p/pg_relation_filenode.md) (dbsize.c:896)
  - [pg_relation_filepath](../p/pg_relation_filepath.md) (dbsize.c:982)
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md) (relcache.c:1375)

## Notes and Other Information
- Returns InvalidRelFileNumber when the OID is not found, which should never happen in normal operation
- The caller is expected to handle the InvalidRelFileNumber case and provide meaningful error reporting
- The function is optimized to check active updates first, ensuring transactional consistency
- Shared and local relation OIDs should never overlap, but the caller must specify which type is expected