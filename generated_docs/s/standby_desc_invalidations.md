# standby_desc_invalidations

## Location
[src/backend/access/rmgrdesc/standbydesc.c:105-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/standbydesc.c#L105-L142)

## Overview
A shared utility function that formats cache invalidation messages from WAL records into human-readable descriptions for debugging and monitoring purposes.

## Definition

```c
void
standby_desc_invalidations(StringInfo buf,
						   int nmsgs, SharedInvalidationMessage *msgs,
						   Oid dbId, Oid tsId,
						   bool relcacheInitFileInval)
```
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
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md) (struct type)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
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

## Simplified Source

```c
void
standby_desc_invalidations(StringInfo buf,
                          int nmsgs, SharedInvalidationMessage *msgs,
                          Oid dbId, Oid tsId,
                          bool relcacheInitFileInval)
{
    // Return early if no invalidation messages
    if (nmsgs <= 0)
        return;

    // Show relcache init file invalidation if applicable
    if (relcacheInitFileInval)
        appendStringInfo(buf, "; relcache init file inval dbid %u tsid %u", dbId, tsId);

    // Process each invalidation message
    appendStringInfoString(buf, "; inval msgs:");
    for (int i = 0; i < nmsgs; i++) {
        SharedInvalidationMessage *msg = &msgs[i];

        if (msg->id >= 0)
            appendStringInfo(buf, " catcache %d", msg->id);
        else if (msg->id == SHAREDINVALCATALOG_ID)
            appendStringInfo(buf, " catalog %u", msg->cat.catId);
        else if (msg->id == SHAREDINVALRELCACHE_ID)
            appendStringInfo(buf, " relcache %u", msg->rc.relId);
        else if (msg->id == SHAREDINVALSMGR_ID)
            appendStringInfoString(buf, " smgr");
        else if (msg->id == SHAREDINVALRELMAP_ID)
            appendStringInfo(buf, " relmap db %u", msg->rm.dbId);
        else if (msg->id == SHAREDINVALSNAPSHOT_ID)
            appendStringInfo(buf, " snapshot %u", msg->sn.relId);
        else
            appendStringInfo(buf, " unrecognized id %d", msg->id);
    }
}
```