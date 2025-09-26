# get_variable_range

## Location
src/backend/utils/adt/selfuncs.c: 5963 - 6089

## Overview
Estimates the minimum and maximum values of a specified variable using statistical data from pg_statistic, with support for different sorting operators and collations.

## Definition


## Detailed Description
This function attempts to determine the range (minimum and maximum values) of a database column or expression by analyzing available statistical information. It employs multiple strategies to find the most appropriate range data:

1. **Histogram Analysis**: First tries to use histogram data with the exact ordering operator requested, extracting the first and last values as min/max
2. **Alternative Histogram**: If no matching histogram exists, scans any available histogram to find extremal values according to the requested ordering
3. **Most Common Values (MCV) Analysis**: Examines MCV data for extreme values, with special logic to determine if MCVs alone represent a complete data distribution

The function includes security checks to ensure the user has permission to access the statistical data, and handles data type-specific operations like datum copying and comparison operators.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : VariableStatData structure with statistical information and metadata about the variable
- : Object identifier for the comparison operator to use (typically "<" for ascending order)
- : Collation to use for comparisons, important for text data types
- : Output parameter for the estimated minimum value
- : Output parameter for the estimated maximum value

## Dependencies
- Functions called/Symbols referenced:
  - statistic_proc_security_check (security permission verification)
  - get_opcode (retrieve function OID for operator)
  - get_typlenbyval (get type storage information)
  - get_attstatsslot (retrieve statistical data slots)
  - datumCopy (safely copy datum values)
  - get_stats_slot_range (scan statistics for range values)
  - free_attstatsslot (cleanup statistical data slots)
- Called from (representative examples):
  - mergejoinscansel (merge join selectivity estimation)

## Notes and Other Information
- Returns true if successful in finding range data, false if no statistical information is available
- The function includes disabled code (NOT_USED) for potentially using actual index probes to get precise min/max values, which was deemed too expensive for frequent use during join planning
- For MCV-only scenarios, the function checks if MCVs represent nearly the complete dataset (>99.999%) before using them for range estimation
- Security checks prevent unauthorized access to statistical data, ensuring the function respects database access controls
- The function handles different collations properly, which is crucial for text data where sorting order depends on locale
- Histogram data is preferred when available as it typically provides more accurate range information than MCV data alone
- The implementation carefully manages memory by copying datum values and freeing statistical slots after use