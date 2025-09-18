# standby_desc_invalidations

## Location
[src/backend/access/rmgrdesc/standbydesc.c:105-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/standbydesc.c#L105-L142)

## Overview
A shared utility function that formats cache invalidation messages from WAL records into human-readable descriptions for debugging and monitoring purposes.

## Definition


## Detailed Description
This function provides detailed formatting of cache invalidation messages found in WAL records. It's designed to be reusable and is called from both standby WAL record descriptions and transaction commit/prepare descriptions, avoiding code duplication.

The function handles multiple types of invalidation messages:
- **Catalog cache invalidations**: References to specific catalog cache entries
- **Catalog invalidations**: Broad catalog invalidations by catalog ID
- **Relation cache invalidations**: Specific relation cache entries  
- **Storage manager invalidations**: SMgr cache invalidations
- **Relation map invalidations**: Relation-to-filenode mapping invalidations
- **Snapshot invalidations**: Snapshot-related cache invalidations

Additionally, it handles relation cache initialization file invalidations, which require database and tablespace ID information.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted description to
- `nmsgs`: Number of invalidation messages in the msgs array
- `msgs`: Array of SharedInvalidationMessage structures containing the invalidation data
- `dbId`: Database OID for relcache init file invalidations
- `tsId`: Tablespace OID for relcache init file invalidations  
- `relcacheInitFileInval`: Boolean indicating whether relcache init file invalidation occurred

## Dependencies
- Functions called/Symbols referenced:
  - SharedInvalidationMessage (struct type)
  - appendStringInfo
  - appendStringInfoString
  - SHAREDINVALCATALOG_ID
  - SHAREDINVALRELCACHE_ID
  - SHAREDINVALSMGR_ID
  - SHAREDINVALRELMAP_ID
  - SHAREDINVALSNAPSHOT_ID
- Called from (representative examples):
  - [standby_desc](standby_desc.md)
  - [xact_desc_commit](../x/xact_desc_commit.md)
  - [xact_desc_prepare](../x/xact_desc_prepare.md)
  - [xact_desc](../x/xact_desc.md)

## Notes and Other Information
- Shared utility function used by both standby and transaction WAL record descriptions
- Returns early if no invalidation messages are present (nmsgs <= 0)
- Handles both expected and unexpected invalidation message types gracefully
- Provides detailed type-specific formatting for each invalidation message type
- Essential for debugging cache coherency issues in replication and transaction processing
- Part of PostgreSQL's cache invalidation infrastructure for maintaining consistency