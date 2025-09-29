# CallSyscacheCallbacks

## Location
[src/backend/utils/cache/inval.c:1580-1606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1580-L1606)

## Overview
Invokes all registered callback functions for a specific system cache when invalidation events occur.

## Definition
```c
void CallSyscacheCallbacks(int cacheid, uint32 hashvalue)
```

## Detailed Description
CallSyscacheCallbacks is the central dispatch function that invokes all callback functions previously registered for a specific system cache via CacheRegisterSyscacheCallback. This function is called when cache invalidation events occur, ensuring that all dependent subsystems are notified and can update their derived data structures accordingly.

Key characteristics:
- **Chain traversal**: Walks through the linked list of callbacks registered for the specified cache ID
- **Ordered execution**: Executes callbacks in registration order (older callbacks first) 
- **Parameter passing**: Passes the cache ID, hash value of the invalidated tuple, and the original registration argument to each callback
- **Validation**: Validates cache ID bounds and ensures callback chain integrity with assertions
- **Export interface**: Specifically exported to allow CatalogCacheFlushCatalog to call it without this module needing to know catalog-to-cache mappings

The function uses the syscache_callback_links array to find the head of the callback chain for the given cache, then traverses the linked list calling each registered function.

## Parameters / Member Variables
- `cacheid`: Integer identifier of the system cache that experienced invalidation (must be valid cache ID < SysCacheSize)
- `hashvalue`: 32-bit hash value of the tuple being invalidated (zero indicates cache reset requiring full flush)

## Dependencies
- Functions called/Symbols referenced:
  - SysCacheSize (global variable for cache count validation)
  - syscache_callback_links (array mapping cache IDs to callback chains)
  - syscache_callback_list (global array storing callback information)
  - [SYSCACHECALLBACK](../S/SYSCACHECALLBACK.md) (structure type for callback entries)
  - elog (error logging function)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [CatalogCacheFlushCatalog](CatalogCacheFlushCatalog.md) (src/backend/utils/cache/catcache.c:851)
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md) (src/backend/utils/cache/inval.c:716)

## Notes and Other Information
- This function is the execution counterpart to CacheRegisterSyscacheCallback (registration)
- The export design allows modular separation between cache invalidation logic and catalog knowledge
- [Hash](../H/Hash.md) value zero has special meaning indicating a complete cache reset
- Each callback receives its original registration argument, allowing for stateful callback implementations
- Essential for coordinating cache invalidation across all PostgreSQL subsystems that depend on system catalog data
- Callbacks are executed synchronously in the context of the invalidation event
- The linked list design allows efficient addition of callbacks while maintaining call order during execution

## Simplified Source

```c
// Invoke all registered callbacks for a system cache invalidation
void CallSyscacheCallbacks(int cacheid, uint32 hashvalue)
{
    int i;

    // Validate cache ID
    if (cacheid < 0 || cacheid >= SysCacheSize)
        elog(ERROR, "invalid cache ID: %d", cacheid);

    // Walk the callback chain for this cache
    i = syscache_callback_links[cacheid] - 1;
    while (i >= 0)
    {
        struct SYSCACHECALLBACK *ccitem = syscache_callback_list + i;

        Assert(ccitem->id == cacheid);

        // Call the callback function
        ccitem->function(ccitem->arg, cacheid, hashvalue);

        // Move to next callback in chain
        i = ccitem->link - 1;
    }
}
```