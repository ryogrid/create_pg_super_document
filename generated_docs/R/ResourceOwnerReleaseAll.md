# ResourceOwnerReleaseAll

## Location
[src/backend/utils/resowner/resowner.c:340-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L340-L412)

## Overview
The  function releases all resources of a specific phase from a ResourceOwner, calling the appropriate cleanup callbacks and optionally reporting resource leaks.

## Definition

```c
struct ResourceOwnerData));
```
## Detailed Description
This function is the core resource cleanup mechanism that releases resources belonging to a specific release phase. It operates on the assumption that the resources have already been sorted by , ensuring proper release order.

The function works by:
1. **Resource Location**: Determines whether resources are stored in the fixed-size array or hash table (after sorting, only one will contain data)
2. **Reverse Iteration**: Processes resources from the end of the array backwards, following the reverse priority order established by sorting
3. **Phase Filtering**: Only releases resources that match the current release phase, stopping when it encounters resources from a different phase
4. **Leak Detection**: Optionally prints warning messages for resources that weren't properly cleaned up (indicating potential resource leaks)
5. **Resource Cleanup**: Calls the  callback for each applicable resource
6. **Count Management**: Updates the appropriate counter (narr or nhash) to reflect the reduced number of remaining resources

The reverse iteration approach is efficient because resources are pre-sorted by phase and priority, allowing the function to process all resources of the current phase in one contiguous block at the end of the array.

## Parameters / Member Variables
- : Pointer to the ResourceOwner structure containing the resources to release
- : The ResourceReleasePhase identifier indicating which phase of resources should be released
- : Boolean flag controlling whether to log warnings for unreleased resources (useful for debugging resource leaks)

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceReleasePhase](ResourceReleasePhase.md) (enum type)
  - [ResourceOwner](ResourceOwner.md) (struct type)
  - [ResourceElem](ResourceElem.md) (struct type)
  - [ResourceOwnerDesc](ResourceOwnerDesc.md) (struct type)
- Called from (representative examples):
  - [ResourceOwnerReleaseInternal](ResourceOwnerReleaseInternal.md) (as part of the multi-phase resource cleanup process)

## Notes and Other Information
- This is a static function accessible only within the resowner.c compilation unit
- Requires that  has been called previously (verified by assertions on owner->releasing and owner->sorted flags)
- The function includes extensive assertions to validate the internal state and ensure proper usage
- Uses the DebugPrint callback if available for more informative leak warnings, otherwise falls back to generic pointer formatting
- The function is designed to be called multiple times for different phases, progressively cleaning up all resources
- Memory allocated for debug strings is properly freed using  to prevent memory leaks during error reporting
- Critical for transaction rollback, connection cleanup, and error recovery scenarios where resources must be released in proper dependency order

## Simplified Source

```c
// Simplified version of ResourceOwnerReleaseAll
static void
ResourceOwnerReleaseAll(ResourceOwner owner, ResourceReleasePhase phase,
                        bool printLeakWarnings)
{
    ResourceElem *items;
    uint32 nitems;

    // Determine resource storage location (array or hash)
    if (owner->nhash == 0) {
        items = owner->arr;
        nitems = owner->narr;
    } else {
        items = owner->hash;
        nitems = owner->nhash;
    }

    // Release resources in reverse order (highest priority first)
    while (nitems > 0) {
        uint32 idx = nitems - 1;
        Datum value = items[idx].item;
        const ResourceOwnerDesc *kind = items[idx].kind;

        // Stop if we've moved past current phase
        if (kind->release_phase > phase)
            break;

        // Optional leak warning for debugging
        if (printLeakWarnings) {
            char *res_str = kind->DebugPrint ?
                kind->DebugPrint(value) :
                psprintf("%s %p", kind->name, DatumGetPointer(value));
            elog(WARNING, "resource was not closed: %s", res_str);
            pfree(res_str);
        }

        // Actually release the resource
        kind->ReleaseResource(value);
        nitems--;
    }

    // Update the appropriate counter
    if (owner->nhash == 0)
        owner->narr = nitems;
    else
        owner->nhash = nitems;
}
```

Key simplifications made:
- Removed detailed assertions for clarity (keeping core algorithm)
- Simplified comments to focus on main logic flow
- Consolidated resource storage determination into clear if-else
- Emphasized the reverse iteration pattern with descriptive comments
- Maintained all essential functionality including leak detection
- Preserved the phase-based filtering logic
- Kept proper resource cleanup and counter management