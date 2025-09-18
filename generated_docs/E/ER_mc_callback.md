# ER_mc_callback

## Location
src/backend/utils/adt/expandedrecord.c: 902 - 926

## Overview
ER_mc_callback is a memory context reset callback function that manages cleanup of tuple descriptor references held by expanded records.

## Definition
static void ER_mc_callback(void *arg)

## Detailed Description
This callback function is registered with the memory context system to ensure proper cleanup of resources when a memory context containing an expanded record is reset or destroyed. Its primary responsibility is to manage the reference count of tuple descriptors that are privately held by expanded record headers.

The function performs reference count management for cached tuple descriptors:
1. **Null pointer safety**: Checks if a tuple descriptor is cached before attempting cleanup
2. **Reference count decrement**: Decrements the tuple descriptors reference count if it is positive (indicating a refcounted descriptor)  
3. **Resource deallocation**: Calls FreeTupleDesc when the reference count reaches zero, indicating no other references exist
4. **Pointer cleanup**: Nullifies the cached tuple descriptor pointer for safety

This callback ensures that tuple descriptors are properly released even if the expanded record is not explicitly destroyed, preventing memory leaks in long-running processes.

## Parameters / Member Variables
- `arg`: Void pointer that is cast to ExpandedRecordHeader*, representing the expanded record that needs cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
- Called from (representative examples):
  - [make_expanded_record_from_typeid](../m/make_expanded_record_from_typeid.md) (callback registration)
  - [make_expanded_record_from_tupdesc](../m/make_expanded_record_from_tupdesc.md) (callback registration)  
  - [make_expanded_record_from_exprecord](../m/make_expanded_record_from_exprecord.md) (callback registration)
  - [expanded_record_fetch_tupdesc](../e/expanded_record_fetch_tupdesc.md) (callback registration)

## Notes and Other Information
- This is a memory context callback, not a regular function call
- Only handles refcounted tuple descriptors (tdrefcount > 0); static descriptors are ignored
- The callback is registered when expanded records need to manage tuple descriptor lifetimes
- Provides automatic cleanup independent of ResourceOwner management
- Critical for preventing memory leaks in PostgreSQL's expanded object system
- The "just for luck" comment indicates defensive programming to avoid dangling pointers