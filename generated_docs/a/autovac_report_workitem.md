# autovac_report_workitem

## Location
[src/backend/postmaster/autovacuum.c:3193-3232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L3193-L3232)

## Overview
Reports autovacuum work item processing activity to PostgreSQL's statistics system for visibility in pg_stat_activity.

## Definition

```c
static void
autovac_report_workitem(AutoVacuumWorkItem *workitem,
						const char *nspname, const char *relname)
```
## Detailed Description
This function provides visibility into autovacuum work item operations by reporting activity to PostgreSQL's statistics collector. It formats descriptive activity strings based on the work item type and updates the process status that appears in pg_stat_activity views.

The function performs these key operations:

1. **Activity String Formatting**: Creates human-readable descriptions of autovacuum work items (currently supports BRIN summarize operations)
2. **Relation Identification**: Includes qualified relation names (schema.table) in the activity description
3. **Block Number Reporting**: Optionally includes specific block numbers when applicable to the work item
4. **Statistics Integration**: Updates both statement timestamp and activity status for monitoring systems

This enables database administrators to monitor autovacuum work item progress through standard PostgreSQL monitoring views.

## Parameters / Member Variables
- `*workitem`: AutoVacuumWorkItem structure containing work item type, target relation, and optional block number
- `*nspname`: Namespace (schema) name of the target relation
- `*relname`: Name of the target relation
## Dependencies
- Functions called/Symbols referenced:
  - BlockNumberIsValid
  - [SetCurrentStatementStartTimestamp](../S/SetCurrentStatementStartTimestamp.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
- Called from (representative examples):
  - [perform_work_item](../p/perform_work_item.md)

## Notes and Other Information
- Currently only supports AVW_BRINSummarizeRange work item type, but the structure allows for easy extension
- Activity strings are limited by MAX_AUTOVAC_ACTIV_LEN to prevent excessive string lengths
- Block numbers are included in the activity string when valid, providing granular progress information
- Uses STATE_RUNNING status to indicate active processing in pg_stat_activity
- The function ensures proper timestamp setting for accurate statistics reporting

## Simplified Source

```c
static void
autovac_report_workitem(AutoVacuumWorkItem *workitem,
                       const char *nspname, const char *relname)
{
    char activity[MAX_AUTOVAC_ACTIV_LEN + 12 + 2];
    char blk[12 + 2];
    int len;

    // Create activity description based on work item type
    switch (workitem->avw_type) {
        case AVW_BRINSummarizeRange:
            snprintf(activity, MAX_AUTOVAC_ACTIV_LEN,
                    "autovacuum: BRIN summarize");
            break;
    }

    len = strlen(activity);

    // Add block number if specified
    if (BlockNumberIsValid(workitem->avw_blockNumber))
        snprintf(blk, sizeof(blk), " %u", workitem->avw_blockNumber);
    else
        blk[0] = '\0';

    // Append qualified relation name and block info
    snprintf(activity + len, MAX_AUTOVAC_ACTIV_LEN - len,
            " %s.%s%s", nspname, relname, blk);

    // Update statistics with current activity
    SetCurrentStatementStartTimestamp();
    pgstat_report_activity(STATE_RUNNING, activity);
}
```