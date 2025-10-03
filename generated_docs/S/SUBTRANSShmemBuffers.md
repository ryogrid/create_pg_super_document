# SUBTRANSShmemBuffers

## Location
[src/backend/access/transam/subtrans.c:201-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L201-L213)

## Overview
Calculates the optimal number of shared memory buffers for the SUBTRANS system, using auto-tuning based on shared buffers or enforcing configured limits.

## Definition
```c
static int
SUBTRANSShmemBuffers(void)
```

## Detailed Description
SUBTRANSShmemBuffers determines the appropriate number of shared memory buffers to allocate for the SUBTRANS (subtransaction) Simple LRU system. The function implements an intelligent auto-tuning mechanism when subtransaction_buffers is set to 0 (auto mode), using a ratio-based calculation that allocates 2MB of SUBTRANS buffers for every 1GB of shared buffers, capped at 8MB.

When a specific buffer count is configured via subtransaction_buffers, the function enforces reasonable bounds by ensuring the value falls between a minimum of 16 buffers and the maximum allowed by the SLRU system (SLRU_MAX_ALLOWED_BUFFERS).

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruAutotuneBuffers](SimpleLruAutotuneBuffers.md) (performs auto-tuning calculation with 512KB/1024KB parameters)
  - SLRU_MAX_ALLOWED_BUFFERS (maximum buffer limit for SLRU systems)
  - subtransaction_buffers (global configuration variable)
- Called from (representative examples):
  - [SUBTRANSShmemSize](SUBTRANSShmemSize.md) (to calculate total shared memory requirements)
  - [SUBTRANSShmemInit](SUBTRANSShmemInit.md) (during SUBTRANS initialization)

## Notes and Other Information
- The auto-tuning algorithm uses SimpleLruAutotuneBuffers(512, 1024) which implements a 2MB per 1GB ratio with an 8MB cap
- Minimum buffer count is enforced at 16 to ensure adequate performance for basic subtransaction operations
- Part of PostgreSQL's shared memory initialization sequence during server startup
- The buffer count directly affects performance of nested transaction operations
- Static function used internally within the SUBTRANS subsystem for memory management

## Simplified Source

```c
// Simplified version of SUBTRANSShmemBuffers
static int SUBTRANSShmemBuffers(void) {
    // Auto-tune based on shared buffers if not explicitly configured
    if (subtransaction_buffers == 0)
        return SimpleLruAutotuneBuffers(512, 1024);

    // Enforce reasonable bounds for configured values
    return Min(Max(16, subtransaction_buffers), SLRU_MAX_ALLOWED_BUFFERS);
}
```

Key simplifications made:
- Added clear comments explaining auto-tuning vs configured mode
- Preserved essential bounds checking and auto-tuning logic
- Maintained the 2MB per 1GB ratio through SimpleLruAutotuneBuffers call
- Focused on the core responsibility: determining optimal buffer count
- Kept the minimum 16 buffer requirement for basic functionality