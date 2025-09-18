# pgstat_drop_subscription

## Location
[src/backend/utils/activity/pgstat_subscription.c:64-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_subscription.c#L64-L74)

## Overview
Schedules the removal of statistics for a logical replication subscription when the drop transaction commits, ensuring proper cleanup of statistics data.

## Definition


## Detailed Description
This function handles the statistics cleanup when a logical replication subscription is dropped. It uses PostgreSQL's transactional statistics mechanism to ensure that the statistics entry for the subscription is only removed if the DROP SUBSCRIPTION transaction commits successfully. This provides ACID compliance for statistics management - if the drop operation fails or is rolled back, the statistics entry remains intact.

The function is designed to work seamlessly with PostgreSQL's transaction system, deferring the actual statistics cleanup until transaction commit time. This prevents premature removal of statistics data that might still be needed if the transaction is aborted.

## Parameters / Member Variables
- : The OID of the subscription being dropped

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_drop_transactional
  - PGSTAT_KIND_SUBSCRIPTION
  - [PgStat_StatSubEntry](../P/PgStat_StatSubEntry.md)
- Called from (representative examples):
  - [DropSubscription](../D/DropSubscription.md)

## Notes and Other Information
This function is typically called during the execution of a DROP SUBSCRIPTION SQL command. The transactional behavior ensures that statistics entries are not orphaned or prematurely deleted. The actual statistics cleanup occurs during transaction commit processing, maintaining consistency with the subscription's lifecycle in the system catalogs. This approach is part of PostgreSQL's broader strategy for maintaining data consistency across system components.