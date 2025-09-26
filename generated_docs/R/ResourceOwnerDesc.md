# ResourceOwnerDesc

## Location
[src/include/utils/resowner.h:91-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/resowner.h#L91-L120)

## Overview
ResourceOwnerDesc is a structure that encapsulates the callbacks and metadata needed for the resource owner system to manage a specific kind of resource.

## Definition
```c
typedef struct ResourceOwnerDesc
{
    const char *name;                    /* name for the object kind, for debugging */

    /* when are these objects released? */
    ResourceReleasePhase release_phase;
    ResourceReleasePriority release_priority;

    /*
     * Release resource.
     */
    void        (*ReleaseResource) (Datum res);

    /*
     * Format a string describing the resource, for debugging purposes.
     */
    char       *(*DebugPrint) (Datum res);

} ResourceOwnerDesc;
```

## Detailed Description
ResourceOwnerDesc defines the interface that resource types must implement to participate in PostgreSQLs resource management system. Each resource type (buffers, locks, files, etc.) provides a ResourceOwnerDesc that tells the resource owner how to manage resources of that type.

The structure defines when and how resources should be released during transaction cleanup. The callback functions are called post-commit or post-abort, so they must only perform noncritical cleanup and cannot fail. The resource owner uses this information to automatically release resources in the correct order and phase.

## Parameters / Member Variables
- `name`: A string identifier for the resource type, used primarily for debugging and error reporting
- `release_phase`: Specifies when during resource cleanup this resource type should be released (before locks, during locks, or after locks)
- `release_priority`: A numeric priority within the release phase that determines the order of resource release relative to other resource types
- `ReleaseResource`: Function pointer to callback that releases a specific resource instance; called automatically during resource owner cleanup
- `DebugPrint`: Optional function pointer to callback that formats a debugging string for a resource instance; if NULL, a generic format is used

## Dependencies
- Functions called/Symbols referenced:
  - ResourceReleasePhase
  - ResourceReleasePriority
  - Datum (for resource values)
- Called from (representative examples):
  - Resource management functions in resowner.c
  - Various subsystems that register resource types (buffer manager, lock manager, file manager, etc.)
  - Test modules for resource owner functionality

## Notes and Other Information
- ResourceOwnerDesc instances are typically defined as static const structures by subsystems that manage resources
- The callbacks must be safe to call during error recovery and transaction abort scenarios
- Extensions can define custom resource types by providing their own ResourceOwnerDesc
- The priority system allows fine-grained control over resource release ordering, which is critical for avoiding circular dependencies during cleanup
- Built-in PostgreSQL resource types use predefined priority constants like RELEASE_PRIO_BUFFER_PINS, RELEASE_PRIO_CATCACHE_REFS, etc.