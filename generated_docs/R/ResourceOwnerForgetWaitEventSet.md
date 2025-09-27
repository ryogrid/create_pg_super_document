# ResourceOwnerForgetWaitEventSet

## Location
[src/backend/storage/ipc/latch.c:219-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L219-L231)

## Overview
A convenience wrapper function that unregisters a WaitEventSet from a ResourceOwner, removing it from automatic cleanup tracking.

## Definition
```c
static inline void
ResourceOwnerForgetWaitEventSet(ResourceOwner owner, WaitEventSet *set)
```

## Detailed Description
This function serves as a convenience wrapper around ResourceOwnerForget, specifically designed for managing WaitEventSet resources. It unregisters a WaitEventSet from a ResourceOwner using the predefined wait_event_set_resowner_desc descriptor. This is typically called when a WaitEventSet is being manually freed before the ResourceOwner is released, ensuring that the ResourceOwner will not attempt to clean up an already-freed resource. The function is implemented as a static inline function for performance, converting the WaitEventSet pointer to a Datum using PointerGetDatum before passing it to the generic ResourceOwnerForget function.

## Parameters / Member Variables
- `owner`: The ResourceOwner that is currently tracking the WaitEventSet resource
- `set`: The WaitEventSet to be unregistered from the ResourceOwner's tracking

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForget](ResourceOwnerForget.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - wait_event_set_resowner_desc (descriptor)
- Called from (representative examples):
  - [FreeWaitEventSet](../F/FreeWaitEventSet.md)

## Notes and Other Information
- This is a static inline function located in src/backend/storage/ipc/latch.c
- Part of PostgreSQL's resource management system that prevents double-free errors
- Works in conjunction with ResourceOwnerRememberWaitEventSet for complete lifecycle management
- Must be called before manually freeing a WaitEventSet to prevent the ResourceOwner from attempting cleanup
- The function uses the same predefined resource descriptor (wait_event_set_resowner_desc) as its counterpart Remember function

## Simplified Source

```c
// Simplified version of ResourceOwnerForgetWaitEventSet
static inline void ResourceOwnerForgetWaitEventSet(ResourceOwner owner, WaitEventSet *set) {
    // Unregister WaitEventSet from ResourceOwner tracking
    // Convert pointer to Datum and use specific descriptor for WaitEventSet resources
    ResourceOwnerForget(owner, PointerGetDatum(set), &wait_event_set_resowner_desc);
}
```

Key simplifications made:
- Added clear comment explaining the core purpose
- Explained the pointer-to-Datum conversion
- Function is already very simple, only formatting and commenting changes
- Preserved the essential resource untracking operation