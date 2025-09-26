# pgstat_assoc_relation

## Location
[src/backend/utils/activity/pgstat_relation.c:132-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L132-L152)

## Overview
Establishes the actual connection between a relation and its statistics tracking infrastructure by creating or finding the appropriate statistics entry and linking it to the relation.

## Definition
```c
void pgstat_assoc_relation(Relation rel)
```

## Detailed Description
This function completes the statistics tracking setup that was initiated by `pgstat_init_relation()`. While the init function only determined whether statistics should be enabled, this function actually allocates the necessary data structures and establishes the connection to the shared statistics system.

The function ensures that a statistics entry exists for the relation before any statistics can be generated. This proactive approach prevents race conditions where another connection might drop the relation and its statistics entry between the time when statistics collection begins and when the first statistics are recorded.

The function operates under strict preconditions: the relation must have statistics enabled and must not already have an associated statistics entry. It calls `pgstat_prep_relation_pending()` to obtain a `PgStat_TableStatus` entry and then establishes a bidirectional link between the relation and the statistics entry.

## Parameters / Member Variables
- `rel`: The Relation object that needs to be associated with statistics tracking infrastructure

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_prep_relation_pending
  - RelationGetRelid (macro)
- Called from (representative examples):
  - pgstat_should_count_relation (via inline check)

## Notes and Other Information
- This function is separate from pgstat_init_relation() because many relation cache entries are opened without ever having statistics reported
- The function enforces a one-to-one relationship between relcache entries and statistics entries through assertions
- The bidirectional linking ensures that both the relation knows about its statistics and the statistics entry knows about its owning relation
- This proactive association prevents race conditions during relation drops in concurrent sessions
- The function requires that statistics are already enabled for the relation (enforced via assertion)
- Performance optimization: statistics structures are only allocated when actually needed for statistics collection