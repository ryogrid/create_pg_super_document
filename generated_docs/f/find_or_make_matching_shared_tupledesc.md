# find_or_make_matching_shared_tupledesc

## Location
[src/backend/utils/cache/typcache.c:2756-2867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2756-L2867)

## Overview
Searches for an existing shared tuple descriptor that matches the given descriptor, or creates a new one if none exists, managing shared record type registries.

## Definition

```c
static TupleDesc
find_or_make_matching_shared_tupledesc(TupleDesc tupdesc)
```
## Detailed Description
This function manages shared tuple descriptors in a multi-process PostgreSQL environment. It first attempts to find an existing shared tuple descriptor that matches the input descriptor by searching the shared record table. If found, it returns the existing shared descriptor. If not found, it allocates a new typmod number, creates a shared copy of the tuple descriptor using , and registers it in both the typmod table and record table for future reuse. The function includes proper error handling and cleanup, with transaction-like semantics using PostgreSQL's PG_TRY/PG_CATCH mechanism to ensure consistency. It also handles race conditions where another process might create a matching descriptor concurrently.

## Parameters / Member Variables
- : The tuple descriptor to find or create a shared version of

## Dependencies
- Functions called/Symbols referenced:
  - [dshash_find](../d/dshash_find.md) (searches shared hash tables)
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md) (finds or creates hash table entries)
  - [dshash_release_lock](../d/dshash_release_lock.md) (releases hash table locks)
  - [dshash_delete_key](../d/dshash_delete_key.md) (removes hash table entries)
  - [dsa_get_address](../d/dsa_get_address.md) (converts shared pointers to addresses)
  - [dsa_free](../d/dsa_free.md) (frees shared memory)
  - [share_tupledesc](../s/share_tupledesc.md) (creates shared tuple descriptor copies)
  - [pg_atomic_fetch_add_u32](../p/pg_atomic_fetch_add_u32.md) (atomic increment for typmod generation)
- Data structures used:
  - [SharedRecordTableKey](../S/SharedRecordTableKey.md)
  - [SharedRecordTableEntry](../S/SharedRecordTableEntry.md)
  - [SharedTypmodTableEntry](../S/SharedTypmodTableEntry.md)
  - dsa_pointer
  - [TupleDesc](../T/TupleDesc.md)
- Called from (representative examples):
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md) (when assigning typmod to record types)

## Notes and Other Information
- Returns NULL if not attached to a SharedRecordTypmodRegistry
- Returned tuple descriptors are not reference counted (tdrefcount == -1)
- Shared descriptors exist as long as the backend remains attached to the session
- Includes race condition handling for concurrent descriptor creation
- Uses atomic operations for typmod generation to ensure uniqueness
- Implements proper cleanup in error scenarios to prevent memory leaks
- The function is static and only used within the typcache.c module

## Simplified Source

```c
static TupleDesc find_or_make_matching_shared_tupledesc(TupleDesc tupdesc) {
    // Check if attached to shared registry
    if (CurrentSession->shared_typmod_registry == NULL)
        return NULL;

    // Try to find existing matching tuple descriptor
    SharedRecordTableKey key = {.shared = false, .u.local_tupdesc = tupdesc};
    SharedRecordTableEntry *record_entry = dshash_find(CurrentSession->shared_record_table, &key, false);

    if (record_entry) {
        // Found existing shared descriptor
        dshash_release_lock(CurrentSession->shared_record_table, record_entry);
        TupleDesc result = dsa_get_address(CurrentSession->area, record_entry->key.u.shared_tupdesc);
        return result;
    }

    // Allocate new typmod and create shared copy
    uint32 typmod = pg_atomic_fetch_add_u32(&CurrentSession->shared_typmod_registry->next_typmod, 1);
    dsa_pointer shared_dp = share_tupledesc(CurrentSession->area, tupdesc, typmod);

    // Register in typmod table
    PG_TRY();
    {
        SharedTypmodTableEntry *typmod_entry =
            dshash_find_or_insert(CurrentSession->shared_typmod_table, &typmod, &found);
        if (found)
            elog(ERROR, "cannot create duplicate shared record typmod");

        typmod_entry->typmod = typmod;
        typmod_entry->shared_tupdesc = shared_dp;
        dshash_release_lock(CurrentSession->shared_typmod_table, typmod_entry);
    }
    PG_CATCH();
    {
        dsa_free(CurrentSession->area, shared_dp);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Register in record table (handle race conditions)
    record_entry = dshash_find_or_insert(CurrentSession->shared_record_table, &key, &found);
    if (found) {
        // Someone else created one concurrently - use theirs
        dshash_release_lock(CurrentSession->shared_record_table, record_entry);
        dshash_delete_key(CurrentSession->shared_typmod_table, &typmod);
        dsa_free(CurrentSession->area, shared_dp);

        TupleDesc result = dsa_get_address(CurrentSession->area, record_entry->key.u.shared_tupdesc);
        return result;
    }

    // Store our new descriptor
    record_entry->key.shared = true;
    record_entry->key.u.shared_tupdesc = shared_dp;
    dshash_release_lock(CurrentSession->shared_record_table, record_entry);

    return dsa_get_address(CurrentSession->area, shared_dp);
}
```