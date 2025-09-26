# makeStringInfo

## Location
[src/common/stringinfo.c:41-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/stringinfo.c#L41-L58)

## Overview
Creates a new empty StringInfo data structure by allocating memory and initializing it for dynamic string operations.

## Definition

```c
StringInfo
makeStringInfo(void)
```
## Detailed Description
The  function is a convenience function that creates a new StringInfo object by allocating memory for a StringInfoData structure and initializing it. This function combines memory allocation with initialization in a single call, providing a clean interface for creating StringInfo objects that are ready for immediate use. The function uses PostgreSQL's memory management system (palloc) to allocate the structure and then calls  to set up the initial state.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [initStringInfo](../i/initStringInfo.md) (initialization)
- Called from (representative examples):
  - [build_backup_content](../b/build_backup_content.md)
  - [pg_backup_start](../p/pg_backup_start.md)  
  - [perform_base_backup](../p/perform_base_backup.md)
  - [DoCopyTo](../D/DoCopyTo.md)
  - [NewExplainState](../N/NewExplainState.md)
  - [array_to_json](../a/array_to_json.md)
  - [jsonb_send](../j/jsonb_send.md)
  - [makeStringAggState](makeStringAggState.md)

## Notes and Other Information
- This function is located at src/common/stringinfo.c:41-58
- Returns a pointer to a newly allocated and initialized StringInfo structure
- The returned StringInfo must be freed when no longer needed to avoid memory leaks
- Widely used throughout PostgreSQL for dynamic string building operations, especially in JSON processing, backup operations, and query explanation
- The function abstracts away the complexity of manual allocation and initialization