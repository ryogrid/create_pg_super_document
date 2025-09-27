# set_plan_disabling_options

## Location
[src/backend/tcop/postgres.c:3795-3836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3795-L3836)

## Overview
A utility function that disables specific query execution plan strategies based on single-character command line arguments, used for testing and debugging query optimization behavior.

## Definition

```c
bool
set_plan_disabling_options(const char *arg, GucContext context, GucSource source)
```
## Detailed Description
This function provides a convenient mechanism to disable various query execution strategies through single-character command line arguments. It maps each character to a corresponding "enable_*" GUC parameter and sets it to "false", effectively disabling that particular execution strategy. This functionality is primarily used for testing the query planner's behavior when certain execution methods are unavailable, debugging performance issues, or forcing the planner to choose alternative execution paths.

## Parameters / Member Variables
- : A string containing single-character codes representing execution strategies to disable ('s' for seqscan, 'i' for indexscan, etc.)
- : The GUC context indicating when/how this configuration change is being applied
- : The source of the configuration change (e.g., command line, configuration file, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [SetConfigOption](../S/SetConfigOption.md): Sets individual GUC parameters programmatically to disable execution strategies
  - GucContext: Context type for GUC operations
  - GucSource: Source type for GUC operations
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md): Processes command-line options during postmaster startup
  - [process_postgres_switches](../p/process_postgres_switches.md): Processes various PostgreSQL command-line switches

## Notes and Other Information
- Supported single-character codes and their corresponding strategies:
  - 's': disable sequential scans (enable_seqscan)
  - 'i': disable index scans (enable_indexscan)  
  - 'o': disable index-only scans (enable_indexonlyscan)
  - 'b': disable bitmap scans (enable_bitmapscan)
  - 't': disable TID scans (enable_tidscan)
  - 'n': disable nested loop joins (enable_nestloop)
  - 'm': disable merge joins (enable_mergejoin)
  - 'h': disable hash joins (enable_hashjoin)
- Returns true if the character was recognized and the corresponding option was disabled
- Returns false if the character is not recognized
- Primarily used for debugging and testing query planner behavior under constrained conditions

## Simplified Source

```c
// Simplified version of set_plan_disabling_options
bool set_plan_disabling_options(const char *arg, GucContext context, GucSource source) {
    const char *guc_parameter = NULL;

    // Map single character to corresponding GUC parameter name
    switch (arg[0]) {
        case 's': guc_parameter = "enable_seqscan"; break;      // Sequential scan
        case 'i': guc_parameter = "enable_indexscan"; break;    // Index scan
        case 'o': guc_parameter = "enable_indexonlyscan"; break; // Index-only scan
        case 'b': guc_parameter = "enable_bitmapscan"; break;   // Bitmap scan
        case 't': guc_parameter = "enable_tidscan"; break;      // TID scan
        case 'n': guc_parameter = "enable_nestloop"; break;     // Nested loop join
        case 'm': guc_parameter = "enable_mergejoin"; break;    // Merge join
        case 'h': guc_parameter = "enable_hashjoin"; break;     // Hash join
    }

    // If valid character found, disable the corresponding plan type
    if (guc_parameter) {
        SetConfigOption(guc_parameter, "false", context, source);
        return true;
    }

    // Character not recognized
    return false;
}
```

Key simplifications made:
- Renamed variable `tmp` to more descriptive `guc_parameter`
- Added inline comments explaining each scan/join type
- Consolidated the logic flow for better readability
- Removed unnecessary tab formatting in comments
- Made the parameter mapping more explicit with clear variable naming