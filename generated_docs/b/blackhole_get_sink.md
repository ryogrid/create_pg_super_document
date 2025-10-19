# blackhole_get_sink

## Location
[src/backend/backup/basebackup_target.c:194-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_target.c#L194-L202)

## Overview
A specialized get_sink function for the "blackhole" backup target that efficiently discards backup data by simply returning the next sink in the chain without adding any processing layer.

## Definition
```c
static bbsink *blackhole_get_sink(bbsink *next_sink, void *detail_arg)
```

## Detailed Description
This function implements the get_sink interface for the "blackhole" backup target type, which is designed to discard backup data entirely. Rather than creating a new bbsink that forwards data and then discards it, this implementation takes a more efficient approach by simply returning the next_sink parameter unchanged. This means the blackhole target effectively removes itself from the processing chain, allowing data to flow directly to the next sink (or to NULL if this is the terminal sink).

The blackhole target is primarily useful for testing scenarios where backup operations need to be performed without actually storing the data anywhere, or for performance testing where the focus is on backup generation rather than storage.

## Parameters / Member Variables
- `next_sink`: The next bbsink in the processing chain (may be NULL)
- `detail_arg`: Target-specific detail arguments (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [bbsink](bbsink.md) (type)

- Called from (representative examples):
  - Referenced in builtin_backup_targets array
  - Called via function pointer from BaseBackupGetSink

## Notes and Other Information
- Static function, not exported outside the compilation unit
- Part of the built-in backup target types initialized by initialize_target_list
- More efficient than creating a forwarding bbsink that discards data
- The detail_arg parameter is ignored since blackhole targets don't require configuration
- Paired with reject_target_detail function which ensures no target details are provided
- Useful for testing backup generation performance without storage overhead
- Demonstrates the flexibility of the bbsink chaining architecture
- The "blackhole" target name is registered in the builtin_backup_targets array

## Simplified Source

```c
static bbsink *blackhole_get_sink(bbsink *next_sink, void *detail_arg) {
    // Blackhole target discards data by not adding any processing layer
    // Simply return the next sink to maintain the chain
    return next_sink;
}
```