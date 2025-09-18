# CheckForBufferLeaks

## Location
[src/backend/storage/buffer/bufmgr.c:3608-3653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3608-L3653)

## Overview
CheckForBufferLeaks is a debugging function that verifies no buffer pins remain held by the current backend process, helping detect buffer pin leaks during development.

## Definition
static void CheckForBufferLeaks(void)

## Detailed Description
This static function serves as a debugging cross-check to ensure that no buffer pins remain held by the current backend process. Since PostgreSQL 8.0, buffer pins should be automatically released by the ResourceOwner mechanism, making this function primarily a development and debugging tool. The function examines both the static PrivateRefCountArray and the overflow hash table (PrivateRefCountHash) to detect any remaining buffer references. When leaks are found, it logs warnings with detailed information about the leaked buffers and triggers an assertion failure in debug builds.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - PrivateRefCountEntry (reference count entry structure)
  - REFCOUNT_ARRAY_ENTRIES (size constant for the static array)
  - [DebugPrintBufferRefcount](../D/DebugPrintBufferRefcount.md) (generates detailed buffer information)
  - HASH_SEQ_STATUS (hash table iteration state)
  - [hash_seq_init](../h/hash_seq_init.md) (initializes hash table iteration)
  - [hash_seq_search](../h/hash_seq_search.md) (iterates through hash table entries)
- Called from (representative examples):
  - [AtEOXact_Buffers](../A/AtEOXact_Buffers.md)
  - [AtProcExit_Buffers](../A/AtProcExit_Buffers.md)

## Notes and Other Information
- This is a static function only accessible within bufmgr.c
- Only active when USE_ASSERT_CHECKING is defined (debug builds)
- Checks both the static PrivateRefCountArray (for normal cases) and the overflow hash table PrivateRefCountHash (when reference counts exceed array capacity)
- Issues WARNING log messages for each detected buffer leak with detailed buffer information
- Triggers an assertion failure if any leaks are found, helping developers identify buffer management bugs
- Essential debugging tool for maintaining the integrity of PostgreSQL's buffer management system
- Works in conjunction with DebugPrintBufferRefcount to provide detailed leak information
- Part of the comprehensive buffer leak detection system introduced to ensure proper resource cleanup