# MemoizeHash_equal

## Location
[src/backend/executor/nodeMemoize.c:221-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L221-L282)

## Overview
Equality function for confirming hash value matches during hash table lookups in the Memoize executor node, comparing cached tuple keys with probe values.

## Definition
```c
static bool MemoizeHash_equal(struct memoize_hash *tb, const MemoizeKey *key1, const MemoizeKey *key2)
```

## Detailed Description
This function performs equality comparison between a cached tuple (key1) and the current probe values stored in the MemoizeState's probeslot. The key2 parameter is unused, as the function always compares against the probeslot contents that were previously populated by prepare_probe_slot().

The function supports two comparison modes:

1. **Binary mode**: Performs fast binary comparison using `datum_image_eq()` after extracting all attributes from both slots. It compares NULL flags first, then performs binary datum comparison for non-NULL values.

2. **Standard mode**: Uses PostgreSQL's expression evaluation system (`ExecQual()`) with the pre-compiled cache_eq_expr, setting the cached tuple as inner and probe tuple as outer.

## Parameters / Member Variables
- `tb`: Pointer to the memoize hash table structure containing private data
- `key1`: MemoizeKey containing the cached tuple parameters to compare against
- `key2`: MemoizeKey pointer (unused - function uses probeslot instead)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoizeKey](MemoizeKey.md)
  - [MemoizeState](MemoizeState.md)
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md)
  - slot_getallattrs
  - [datum_image_eq](../d/datum_image_eq.md)
  - ExecQual
- Called from (representative examples):
  - SH_DECLARE (hash table declaration)
  - SH_EQUAL (hash table equality macro)

## Notes and Other Information
- The key2 parameter is intentionally unused; comparisons are always against the probeslot
- Assumes probeslot has already been populated by prepare_probe_slot()
- Binary mode provides better performance for types supporting binary comparison
- Uses memory context switching to ecxt_per_tuple_memory in binary mode
- In standard mode, leverages PostgreSQL's expression evaluation for complex equality semantics
- Short-circuits on NULL mismatches and continues on both-NULL cases