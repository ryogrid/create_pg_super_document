# spgGetCache

## Location
[src/backend/access/spgist/spgutils.c:182-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L182-L308)

## Overview
spgGetCache fetches and initializes the local cache of SP-GiST access method-specific information about an index, creating and configuring the cache if it doesn't already exist.

## Definition


## Detailed Description
This function manages the SP-GiST cache (stored in rd_amcache) for an index relation. If the cache doesn't exist, it creates a new SpGistCache structure and populates it with configuration information obtained from the opclass config function, type descriptions for various data types used by the index, and metadata from the index's metapage.

The function performs several key operations during cache initialization:
1. Validates that the index has exactly one key column (SP-GiST requirement)
2. Determines the nominal input data type using GetIndexInputType
3. Calls the opclass config function to get SP-GiST-specific configuration
4. Handles leafType determination, including binary coercion checks
5. Validates compress method requirements when leaf type differs from input type
6. Fills type descriptors for attribute, leaf, prefix, and label types
7. For real (non-partitioned) indexes, reads metadata from the metapage

## Parameters / Member Variables
- : The relation representing the SP-GiST index

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (allocate zeroed cache structure)
  - IndexRelationGetNumberOfKeyAttributes, IndexRelationGetNumberOfAttributes (index validation)
  - [GetIndexInputType](../G/GetIndexInputType.md) (determine nominal input type)
  - [index_getprocinfo](../i/index_getprocinfo.md) (get opclass procedure info)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (call opclass config function)
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md) (check type coercion compatibility)
  - [fillTypeDesc](../f/fillTypeDesc.md) (populate type descriptors)
  - [index_getprocid](../i/index_getprocid.md) (check for compress procedure)
  - [ReadBuffer](../R/ReadBuffer.md), LockBuffer, SpGistPageGetMeta (metapage access)
  - Constants: spgKeyColumn, SPGIST_CONFIG_PROC, SPGIST_COMPRESS_PROC, etc.
- Called from (representative examples):
  - [spgcanreturn](spgcanreturn.md) (at src/backend/access/spgist/spgscan.c:1092)
  - [initSpGistState](../i/initSpGistState.md) (at src/backend/access/spgist/spgutils.c:347)
  - [allocNewBuffer](../a/allocNewBuffer.md) (at src/backend/access/spgist/spgutils.c:507)
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md) (at src/backend/access/spgist/spgutils.c:563)
  - [SpGistSetLastUsedPage](../S/SpGistSetLastUsedPage.md) (at src/backend/access/spgist/spgutils.c:667)

## Notes and Other Information
- Located in src/backend/access/spgist/spgutils.c:182-308
- The cache is stored in the relation's rd_amcache field for efficient reuse
- SP-GiST indexes must have exactly one key column but can have INCLUDE columns
- Handles polymorphic opclasses by passing the actual input type to the config function
- Includes validation that compress method is defined when leaf type differs from input type
- For partitioned indexes, skips metapage reading since they don't have physical storage
- The function implements lazy initialization - cache is created only when first needed
- Type descriptors are cached for efficient access during index operations