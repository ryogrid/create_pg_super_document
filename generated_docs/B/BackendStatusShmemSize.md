# BackendStatusShmemSize

## Location
[src/backend/utils/activity/backend_status.c:83-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L83-L115)

## Overview
Calculates the total shared memory space required for the backend status tracking infrastructure during PostgreSQL initialization.

## Definition

```c
Size
BackendStatusShmemSize(void)
```
## Detailed Description
This function computes the memory requirements for all components of the backend status system that will be allocated in shared memory. It calculates space for the main backend status array and various string buffers used to track backend activity information. The calculation includes conditional allocations for SSL and GSS status information when those features are enabled.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md)
  - [add_size](../a/add_size.md)
  - [PgBackendStatus](../P/PgBackendStatus.md)
  - NumBackendStatSlots
  - NAMEDATALEN
  - pgstat_track_activity_query_size
  - [PgBackendSSLStatus](../P/PgBackendSSLStatus.md) (ifdef USE_SSL)
  - [PgBackendGSSStatus](../P/PgBackendGSSStatus.md) (ifdef ENABLE_GSS)
- Called from:
  - [CalculateShmemSize](../C/CalculateShmemSize.md)

## Notes and Other Information
The function uses PostgreSQL's safe arithmetic functions (mul_size, add_size) to prevent integer overflow when calculating memory requirements. It conditionally includes memory for SSL and GSS status buffers based on compile-time configuration. This function is called during postmaster startup to determine how much shared memory to allocate for the backend status system.

## Simplified Source

```c
// Simplified version of BackendStatusShmemSize
Size BackendStatusShmemSize(void) {
    Size total_size;

    // Core component 1: Backend status array
    total_size = sizeof(PgBackendStatus) * NumBackendStatSlots;

    // Core component 2: Application name buffer
    total_size += NAMEDATALEN * NumBackendStatSlots;

    // Core component 3: Client hostname buffer
    total_size += NAMEDATALEN * NumBackendStatSlots;

    // Core component 4: Activity query buffer
    total_size += pgstat_track_activity_query_size * NumBackendStatSlots;

    // Optional component 5: SSL status buffer (if SSL enabled)
    if (SSL_ENABLED) {
        total_size += sizeof(PgBackendSSLStatus) * NumBackendStatSlots;
    }

    // Optional component 6: GSS status buffer (if GSS enabled)
    if (GSS_ENABLED) {
        total_size += sizeof(PgBackendGSSStatus) * NumBackendStatSlots;
    }

    return total_size;
}
```

Key simplifications made:
- Replaced safe arithmetic functions (mul_size, add_size) with direct arithmetic for clarity
- Converted preprocessor conditionals to pseudo-code if statements
- Used more descriptive variable names and comments
- Simplified the incremental size calculation pattern
- Focused on the core memory allocation logic