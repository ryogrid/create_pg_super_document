# perform_work_item

## Location
[src/backend/postmaster/autovacuum.c:2588-2701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L2588-L2701)

## Overview
perform_work_item executes a previously registered work item that was requested by backends, currently supporting BRIN index summarization operations.

## Definition
```c
static void perform_work_item(AutoVacuumWorkItem *workitem)
```

## Detailed Description
This function processes specific maintenance tasks that have been queued by backends for execution by autovacuum workers. It operates independently from regular table vacuum/analyze operations and handles specialized work items that require background processing. Currently, the primary supported work item type is BRIN (Block Range Index) summarization, which updates summary information for BRIN indexes.

The function includes comprehensive error handling to ensure that failures in one work item do not affect the processing of subsequent items. It maintains proper memory context management and provides detailed error reporting with relation context information.

## Parameters / Member Variables
- `workitem`: Pointer to AutoVacuumWorkItem structure containing:
  - `avw_type`: Type of work item (e.g., AVW_BRINSummarizeRange)
  - `avw_relation`: OID of the target relation
  - `avw_blockNumber`: Block number for block-specific operations
  - `avw_used`: Flag indicating if work item slot is in use
  - `avw_active`: Flag indicating if work item is currently being processed
  - `avw_database`: Database OID where the work should be performed

## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_name](../g/get_rel_name.md)/get_namespace_name/get_database_name (relation name resolution)
  - [autovac_report_workitem](../a/autovac_report_workitem.md) (progress reporting)
  - [brin_summarize_range](../b/brin_summarize_range.md) (BRIN index summarization via DirectFunctionCall2)
  - [MemoryContextReset](../M/MemoryContextReset.md)/MemoryContextSwitchTo (memory management)
  - [AbortOutOfAnyTransaction](../A/AbortOutOfAnyTransaction.md)/StartTransactionCommand (transaction management)
  - [EmitErrorReport](../E/EmitErrorReport.md)/FlushErrorState (error handling)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) (main autovacuum processing loop)

## Notes and Other Information
- Currently supports only AVW_BRINSummarizeRange work item type, with extensible switch statement for future types
- Uses PortalContext for per-work-item memory allocations to ensure proper cleanup
- Does not update did_vacuum flag as these operations are separate from regular vacuum activities
- Includes safety checks for dropped relations by validating relation name resolution
- Implements proper interrupt handling and query cancellation management
- Work item processing is intentionally lossy - failures don't block other work items
- Functions called within work item handlers are responsible for their own user switching and sandboxing

## Simplified Source

```c
static void
perform_work_item(AutoVacuumWorkItem *workitem)
{
    char *cur_datname = NULL;
    char *cur_nspname = NULL;
    char *cur_relname = NULL;

    // Get relation names for error reporting
    cur_relname = get_rel_name(workitem->avw_relation);
    cur_nspname = get_namespace_name(get_rel_namespace(workitem->avw_relation));
    cur_datname = get_database_name(MyDatabaseId);
    if (!cur_relname || !cur_nspname || !cur_datname)
        goto deleted2;

    autovac_report_workitem(workitem, cur_nspname, cur_relname);
    MemoryContextReset(PortalContext);

    // Execute work item with error handling
    PG_TRY();
    {
        MemoryContextSwitchTo(PortalContext);

        // Dispatch based on work item type
        switch (workitem->avw_type)
        {
            case AVW_BRINSummarizeRange:
                DirectFunctionCall2(brin_summarize_range,
                                    ObjectIdGetDatum(workitem->avw_relation),
                                    Int64GetDatum((int64) workitem->avw_blockNumber));
                break;
            default:
                elog(WARNING, "unrecognized work item found: type %d", workitem->avw_type);
                break;
        }

        QueryCancelPending = false;
    }
    PG_CATCH();
    {
        // Handle errors and recover transaction
        HOLD_INTERRUPTS();
        errcontext("processing work entry for relation \"%s.%s.%s\"",
                   cur_datname, cur_nspname, cur_relname);
        EmitErrorReport();
        AbortOutOfAnyTransaction();
        FlushErrorState();
        MemoryContextReset(PortalContext);
        StartTransactionCommand();
        RESUME_INTERRUPTS();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(AutovacMemCxt);

deleted2:
    // Clean up allocated names
    if (cur_datname) pfree(cur_datname);
    if (cur_nspname) pfree(cur_nspname);
    if (cur_relname) pfree(cur_relname);
}
```