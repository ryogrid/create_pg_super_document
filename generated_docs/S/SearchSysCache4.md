# SearchSysCache4

## Location
[src/backend/utils/cache/syscache.c:254-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L254-L268)

## Overview
SearchSysCache4 is a high-level interface function that searches PostgreSQL's system cache for a tuple using four key values.

## Definition
HeapTuple SearchSysCache4(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4)

## Detailed Description
SearchSysCache4 provides a convenient wrapper around the lower-level SearchCatCache4 function for searching system caches with exactly four key values. It validates that the cache exists and has the expected number of keys (4) before delegating to the catalog cache search mechanism. This function is commonly used for accessing operator and procedure information that requires four-part keys for identification.

## Parameters / Member Variables
- cacheId: Integer identifier of the system cache to search in
- key1: First search key value as a Datum
- key2: Second search key value as a Datum
- key3: Third search key value as a Datum
- key4: Fourth search key value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - [SearchCatCache4](SearchCatCache4.md)
- Called from (representative examples):
  - [inclusion_get_strategy_procinfo](../i/inclusion_get_strategy_procinfo.md)
  - [minmax_get_strategy_procinfo](../m/minmax_get_strategy_procinfo.md)
  - [OpernameGetOprid](../O/OpernameGetOprid.md)
  - [OperatorGet](../O/OperatorGet.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)

## Notes and Other Information
- The function includes assertions to validate the cache ID is within bounds and that the specified cache exists
- It specifically validates that the cache is configured for exactly 4 keys before proceeding
- Returns a HeapTuple if found, or NULL if no matching tuple exists in the cache
- Commonly used for operator family and strategy procedure lookups that require four-part identification
- The returned tuple should be released using ReleaseSysCache when no longer needed