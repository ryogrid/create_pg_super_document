# RelationMapFilenumberToOid

## Location
[src/backend/utils/cache/relmapper.c:218-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L218-L264)

## Overview
Performs reverse mapping from relation file number to OID, primarily used for diagnostic and informational purposes when examining the filesystem or transaction logs.

## Definition
Oid RelationMapFilenumberToOid(RelFileNumber filenumber, bool shared)

## Detailed Description
This function provides the inverse operation of RelationMapOidToFilenumber, translating a file number back to its corresponding relation OID. Unlike the forward mapping which is used during normal database operations, this reverse mapping is primarily intended for diagnostic purposes, filesystem analysis, and transaction log examination.

The function follows the same two-tiered search strategy as its counterpart:
1. First searches active update maps for pending changes
2. Falls back to the main mapping tables if not found in updates

This ensures consistency with any uncommitted mapping changes during analysis operations.

## Parameters / Member Variables
- `filenumber`: The RelFileNumber whose corresponding OID is being sought
- `shared`: Boolean flag indicating whether to search shared relation maps (true) or local relation maps (false)

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileNumber](RelFileNumber.md) (parameter type)
  - [RelMapFile](RelMapFile.md) (structure type used for mapping tables)
  - InvalidOid (returned when no mapping is found)
- Called from (representative examples):
  - RelidByRelfilenumber (relfilenumbermap.c:185, 236)

## Notes and Other Information
- Not intended for normal runtime operations but rather for diagnostic and analysis purposes
- Returns InvalidOid when the file number doesn't correspond to a mapped relation, which is common since not all relations use the mapping system
- Particularly useful when examining filesystem contents or analyzing transaction logs
- The reverse mapping can help identify which relation a particular file belongs to during troubleshooting