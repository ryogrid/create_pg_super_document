# InProgressEnt

## Location
src/backend/utils/cache/relcache.c: 168 - 183

## Overview
InProgressEnt is a structure that tracks ongoing RelationBuildDesc() operations, ensuring proper handling of relation cache invalidation during concurrent CREATE INDEX operations.

## Definition


## Detailed Description
InProgressEnt is a critical data structure for PostgreSQL's relation cache consistency mechanism. It maintains a stack (in_progress_list) of ongoing RelationBuildDesc() calls to handle a specific concurrency issue with CREATE INDEX CONCURRENTLY operations.

The problem this structure solves is related to CREATE INDEX CONCURRENTLY, which makes catalog changes under ShareUpdateExclusiveLock. It is critical that each backend absorbs these changes no later than the next transaction start. To ensure this, RelationBuildDesc() uses a retry loop that continues until it finishes building the relation descriptor without receiving a relevant invalidation message.

The in_progress_list acts as a stack where each entry represents an active RelationBuildDesc() call. When invalidation messages arrive during the build process, the corresponding InProgressEnt entry is marked as invalidated, causing RelationBuildDesc() to restart the build process to ensure it sees the most current catalog state.

## Parameters / Member Variables
- : The Object Identifier (OID) of the relation currently being built via RelationBuildDesc()
- : Boolean flag indicating whether an invalidation message arrived for this relation while it was being built, requiring a rebuild

## Dependencies
- Functions called/Symbols referenced:
  - Used in conjunction with static variables: in_progress_list, in_progress_list_len, in_progress_list_maxlen
- Called from (representative examples):
  - RelationBuildDesc (main usage for tracking build progress)
  - RelationCacheInvalidateEntry (sets invalidated flag)
  - RelationCacheInvalidate (sets invalidated flag for bulk invalidations)
  - AtEOXact_RelationCache (cleanup at transaction end)
  - AtEOSubXact_RelationCache (cleanup at subtransaction end)

## Notes and Other Information
- This mechanism is specific to CREATE INDEX CONCURRENTLY and differs from typical invalidation consumers that don't retry operations
- The structure is managed as a stack (LIFO) to handle nested RelationBuildDesc() calls correctly  
- The invalidated flag being set triggers RelationBuildDesc() to restart its relation building process
- Memory for in_progress_list is allocated during RelationCacheInitialize() and grows dynamically as needed
- The list is reset to empty at transaction/subtransaction end during abort scenarios
- Critical for maintaining consistency between concurrent DDL operations and relation cache state