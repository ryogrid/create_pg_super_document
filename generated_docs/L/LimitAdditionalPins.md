# LimitAdditionalPins

## Location
[src/backend/storage/buffer/bufmgr.c:2104-2134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2104-L2134)

## Overview
LimitAdditionalPins constrains the number of additional buffer pins a batch operation can acquire to prevent buffer pool exhaustion and ensure system stability.

## Definition

```c
void
LimitAdditionalPins(uint32 *additional_pins)
```
## Detailed Description
LimitAdditionalPins implements a resource management mechanism that prevents individual backends from monopolizing the buffer pool during batch operations. It calculates a proportional limit based on the total number of buffers (shared_buffers) and the maximum possible number of backends, ensuring that no single operation can pin too many buffers simultaneously.

The function uses a pessimistic approach by assuming maximum backend concurrency and accounts for buffers already pinned by the current backend. It guarantees that at least one additional pin is always allowed to prevent operations from becoming completely blocked. The calculation subtracts already pinned buffers including both overflowed pins and the estimated maximum pins in the private reference count array.

## Parameters / Member Variables
- `*additional_pins`: Pointer to the requested number of additional pins, modified in-place to enforce the calculated limit
## Dependencies
- Functions called/Symbols referenced:
  - MaxBackends (global variable)
  - NUM_AUXILIARY_PROCS
  - NBuffers (global variable)
  - PrivateRefCountOverflowed (global variable)
  - REFCOUNT_ARRAY_ENTRIES
- Called from (representative examples):
  - [read_stream_begin_relation](../r/read_stream_begin_relation.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - RelationGetNumberOfBlocks

## Notes and Other Information
- Always allows at least one additional pin to ensure operations can proceed
- Uses conservative estimates to prevent buffer pool starvation
- Critical for maintaining system stability during large batch operations
- Accounts for both tracked and estimated untracked buffer pins by the current backend
- Part of PostgreSQL's buffer management safety mechanisms

## Simplified Source

```c
void
LimitAdditionalPins(uint32 *additional_pins)
{
    uint32 max_backends;
    int max_proportional_pins;

    // Allow small requests without limitation
    if (*additional_pins <= 1)
        return;

    // Calculate fair share of buffers per backend
    max_backends = MaxBackends + NUM_AUXILIARY_PROCS;
    max_proportional_pins = NBuffers / max_backends;

    // Subtract buffers already pinned by this backend
    // (Use conservative estimate for PrivateRefCountArray)
    max_proportional_pins -= PrivateRefCountOverflowed + REFCOUNT_ARRAY_ENTRIES;

    // Always allow at least one additional pin
    if (max_proportional_pins <= 0)
        max_proportional_pins = 1;

    // Limit the request to our calculated maximum
    if (*additional_pins > max_proportional_pins)
        *additional_pins = max_proportional_pins;
}
```