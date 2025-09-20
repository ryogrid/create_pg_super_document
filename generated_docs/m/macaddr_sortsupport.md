# macaddr_sortsupport

## Location
[src/backend/utils/adt/mac.c:363-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L363-L399)

## Overview
Populates a SortSupport structure with comparison functions and state for optimized sorting of MAC addresses using abbreviated keys when possible.

## Definition

```c
Datum
macaddr_sortsupport(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements PostgreSQL's SortSupport interface for the  data type. It configures a SortSupport structure with appropriate comparison functions and, when abbreviation is enabled, sets up abbreviated key sorting with cardinality estimation using HyperLogLog. The function provides two sorting modes: standard comparison using  and optimized abbreviated key sorting with fallback capability. When abbreviation is used, it initializes tracking state to monitor input cardinality and determine whether abbreviated sorting remains beneficial.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - Pointer to the SortSupport structure to populate

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts SortSupport pointer from function arguments
  -  - Fast comparison function for MAC addresses
  -  - Switches to appropriate memory context
  -  - PostgreSQL memory allocation function
  -  - Initializes HyperLogLog cardinality estimator
  -  - Standard unsigned comparison for abbreviated keys
  -  - Converts MAC addresses to abbreviated keys
  -  - Determines when to abort abbreviated sorting
  -  - Returns void following PostgreSQL conventions
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Sets up both standard and abbreviated key sorting strategies
- Uses HyperLogLog algorithm for cardinality estimation with 10-bit precision
- Memory allocation occurs in the SortSupport context for proper cleanup
- Abbreviated sorting can be dynamically disabled if it becomes ineffective
- The  tracks input statistics for optimization decisions
- This function follows PostgreSQL's V1 calling convention for built-in functions
- Part of PostgreSQL's advanced sorting infrastructure for improved performance