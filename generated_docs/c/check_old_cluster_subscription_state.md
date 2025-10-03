# check_old_cluster_subscription_state

## Location
[src/bin/pg_upgrade/check.c:2003-2117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L2003-L2117)

## Overview
Verifies that all logical replication subscriptions in the old PostgreSQL cluster have valid replication origins and that subscribed tables are in safe states for upgrade.

## Definition
```c
static void check_old_cluster_subscription_state(void)
```

## Detailed Description
This function ensures the integrity of logical replication subscriptions before PostgreSQL cluster upgrade. It performs two critical validations: first, it verifies that each subscription has a corresponding replication origin in the pg_replication_origin catalog; second, it checks that all subscribed table relations are in either 'i' (initialize) or 'r' (ready) state.

The function queries the old cluster's catalogs to identify subscriptions missing replication origins and tables in unsafe synchronization states. Unsafe states include DATASYNC, SYNCDONE, FINISHEDCOPY, and others that could leave dangling slots or origins after upgrade. When problems are detected, detailed information is written to a file and the upgrade process is terminated with comprehensive error messages.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [DbInfo](../D/DbInfo.md)
  - [connectToServer](connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - fopen_priv
  - [PQfinish](../P/PQfinish.md)
  - [pg_log](../p/pg_log.md)
  - [check_ok](check_ok.md)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- Creates "subs_invalid.txt" file in the log base directory when problematic subscriptions are detected
- Only validates replication origins once (during the first database iteration) since they are cluster-wide
- Prevents upgrade when table sync states could result in dangling replication slots or origins
- Supports only 'i' (initialize) and 'r' (ready) table sync states for safe upgrade
- The origin name pattern is 'pg_' + subscription OID for validation
- Terminates upgrade process if any subscription issues are found
- File location: src/bin/pg_upgrade/check.c:2003-2117