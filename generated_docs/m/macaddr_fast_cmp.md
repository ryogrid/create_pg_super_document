# macaddr_fast_cmp

## Location
[src/backend/utils/adt/mac.c:400-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L400-L414)

## Overview
Provides fast comparison of MAC addresses for PostgreSQL's SortSupport infrastructure by extracting Datum values and delegating to the internal comparison function.

## Definition


## Detailed Description
The  function serves as PostgreSQL's SortSupport "traditional" comparison function for MAC addresses. It acts as a bridge between the SortSupport framework and the core MAC address comparison logic. The function extracts two MAC addresses from their Datum representations and delegates the actual comparison to . This function is specifically designed for use within PostgreSQL's sorting infrastructure and provides optimized access to MAC address data during sort operations.

## Parameters / Member Variables
- : First MAC address as a Datum value
- : Second MAC address as a Datum value  
- : SortSupport context (unused in this function but required by interface)

## Dependencies
- Functions called/Symbols referenced:
  -  - Converts Datum to macaddr pointer
  -  - Internal MAC address comparison function
- Called from (representative examples):
  -  - Used as default and fallback comparator
  -  - Referenced in sort support state structure

## Notes and Other Information
- Declared as static function, only accessible within mac.c source file
- Returns integer comparison result: <0 if x < y, 0 if equal, >0 if x > y
- Optimized for use in sorting contexts where Datum extraction is needed
- Part of PostgreSQL's SortSupport infrastructure for improved sorting performance
- The SortSupport parameter is required by the interface but not used in this implementation
- Provides the "traditional" comparison path when abbreviated sorting is not used or available