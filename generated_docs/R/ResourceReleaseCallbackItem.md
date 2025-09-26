# ResourceReleaseCallbackItem

## Location
src/backend/utils/resowner/resowner.c: 180 - 185

## Overview
ResourceReleaseCallbackItem represents an entry in a linked list of callbacks that are invoked during resource owner release operations, enabling dynamically loaded modules to participate in resource cleanup.

## Definition
```c
typedef struct ResourceReleaseCallbackItem
{
    struct ResourceReleaseCallbackItem *next;
    ResourceReleaseCallback callback;
    void       *arg;
} ResourceReleaseCallbackItem;
```

## Detailed Description
ResourceReleaseCallbackItem serves as a node in a linked list that maintains callbacks for resource release operations. This mechanism allows dynamically loaded modules and extensions to register cleanup functions that will be called during transaction commit or abort phases.

The callback system provides a flexible extension point for modules that need to perform cleanup operations that are not directly managed by the core resource owner system. Each callback receives context about the release phase (before locks, locks, after locks), whether the operation is a commit, and whether it's happening at the top level.

The linked list structure allows for efficient addition and removal of callbacks without requiring dynamic array management, and callbacks are invoked in registration order during resource release.

## Parameters / Member Variables
- `next`: Pointer to the next ResourceReleaseCallbackItem in the linked list, NULL for the last item
- `callback`: Function pointer of type ResourceReleaseCallback that will be invoked during resource release
- `arg`: Opaque pointer argument that will be passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - ResourceReleaseCallback (function pointer type: `void (*)(ResourceReleasePhase, bool, bool, void*)`)
- Called from (representative examples):
  - ResourceOwnerReleaseInternal (during callback invocation)
  - RegisterResourceReleaseCallback (for adding new callbacks)
  - UnregisterResourceReleaseCallback (for removing callbacks)

## Notes and Other Information
- ResourceReleaseCallback signature: `void (*ResourceReleaseCallback)(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)`
- ResourceReleasePhase has three values: RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS, RESOURCE_RELEASE_AFTER_LOCKS
- Callbacks are invoked during resource cleanup phases to give modules control over their cleanup ordering
- The linked list approach provides O(1) insertion but O(n) removal, suitable for the typical use pattern
- Callback functions must not fail as they are called during cleanup operations (post-commit or post-abort)
- The system supports multiple callbacks with the same function but different arguments