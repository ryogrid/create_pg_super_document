# hash_freeze

## Location
[src/backend/utils/hash/dynahash.c:1494-1510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1494-L1510)

## Overview
Freezes a hash table against future insertions while still allowing deletions, preventing bucket splits and eliminating the need for careful sequential scan management.

## Definition

```c
void
hash_freeze(HTAB *hashp)
```
## Detailed Description
The hash_freeze function marks a hash table as frozen, preventing any future insertions that could cause bucket splits. This is a performance optimization that simplifies sequential scan management by eliminating the need to track and deregister scans properly. Once frozen, the hash table structure becomes stable, allowing callers to perform sequential scans without worrying about calling hash_seq_term at precise moments. The function includes safety checks to prevent freezing shared hash tables or tables with active scans, as these operations could lead to inconsistent states.

## Parameters / Member Variables
- : Pointer to the HTAB (hash table) structure to be frozen

## Dependencies
- Functions called/Symbols referenced:
  - [has_seq_scans](has_seq_scans.md)
  - [HTAB](../H/HTAB.md) (structure access)
  - elog (for error reporting)
- Called from (representative examples):
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md) (referenced in header file)

## Notes and Other Information
- Cannot freeze shared hash tables - will throw an ERROR
- Cannot freeze tables with active sequential scans - will throw an ERROR  
- Multiple calls to hash_freeze() on the same table are allowed and safe
- Once frozen, deletions are still permitted but insertions are prevented
- This optimization is particularly useful for hash tables that will only be read from after a certain point
- Part of the PostgreSQL dynamic hash table implementation in dynahash.c