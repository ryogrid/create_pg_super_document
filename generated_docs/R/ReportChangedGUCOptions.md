# ReportChangedGUCOptions

## Location
[src/backend/utils/misc/guc.c:2598-2635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2598-L2635)

## Overview
ReportChangedGUCOptions reports recently-changed PostgreSQL configuration variables marked with GUC_REPORT to the frontend client, ensuring efficient parameter status communication.

## Definition
void ReportChangedGUCOptions(void)

## Detailed Description
This function is called just before PostgreSQL waits for a new client query to transmit updates for configuration parameters that have changed since the last report. It implements an efficient reporting mechanism that ensures each variable is reported at most once per query, even if it changed multiple times during query execution.

Key features include:
1. Quick exit if reporting is not yet enabled
2. Special handling for the in_hot_standby parameter using a hack to detect recovery state changes
3. Iteration through the guc_report_list to process variables flagged with GUC_NEEDS_REPORT
4. Automatic cleanup of the report list after processing each variable

The function is designed to handle scenarios where configuration values may change multiple times within a single query (such as with function SET clauses) but eventually revert to their original values, avoiding redundant parameter status messages.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](RecoveryInProgress.md): Checks if database is in recovery mode
  - [SetConfigOption](../S/SetConfigOption.md): Sets configuration option value
  - slist_foreach_modify: Iterates through singly-linked list with modification capability
  - slist_container: Extracts container structure from list node
  - [ReportGUCOption](ReportGUCOption.md): Reports individual GUC option to frontend
  - [slist_delete_current](../s/slist_delete_current.md): Removes current item from list during iteration
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md): Called in main query processing loop in src/backend/tcop/postgres.c:4682

## Notes and Other Information
- Only operates when reporting_enabled flag is true (set by BeginReportingGUCOptions)
- Uses the global guc_report_list singly-linked list to track variables needing reports
- Includes special hack for in_hot_standby parameter since it's not changed by normal GUC actions
- The hack assumes in_hot_standby can never transition from false to true (only true to false)
- Clears the GUC_NEEDS_REPORT status flag after reporting each variable
- Essential for PostgreSQL protocol compliance and client session management
- Optimizes network traffic by batching parameter updates and avoiding duplicate reports

## Simplified Source

```c
// Simplified version of ReportChangedGUCOptions
void ReportChangedGUCOptions(void) {
    // Quick exit if reporting not enabled
    if (!reporting_enabled)
        return;

    // Special case: Handle in_hot_standby parameter transition
    // This parameter can only go from true to false, never false to true
    if (in_hot_standby_guc && !RecoveryInProgress()) {
        SetConfigOption("in_hot_standby", "false",
                       PGC_INTERNAL, PGC_S_OVERRIDE);
    }

    // Process all variables that need reporting
    slist_mutable_iter iter;
    slist_foreach_modify(iter, &guc_report_list) {
        // Get the configuration variable from the list
        struct config_generic *conf = slist_container(struct config_generic,
                                                     report_link, iter.cur);

        // Report the variable to the client
        ReportGUCOption(conf);

        // Clear the "needs report" flag
        conf->status &= ~GUC_NEEDS_REPORT;

        // Remove from the report list
        slist_delete_current(&iter);
    }
}
```

Key simplifications made:
- Removed detailed comments explaining the rationale (kept essential ones)
- Consolidated variable declarations closer to usage
- Simplified the loop structure explanation
- Removed Assert statement for clarity
- Focused on the main execution flow
- Added brief inline comments for each major step