# GetNewObjectId

## Location
src/backend/access/transam/varsup.c: 555 - 622

## Overview
Allocates new Object Identifiers (OIDs) from a cluster-wide counter, handling wraparound detection and WAL logging with prefetch optimization for performance.

## Definition
```c
Oid GetNewObjectId(void)
```

## Detailed Description
This function serves as the core OID allocation mechanism in PostgreSQL, managing a global 32-bit counter that provides unique identifiers for database objects. The function implements several critical safeguards and optimizations:

**Wraparound Management**: Since OIDs are only 32 bits wide, eventual wraparound is inevitable. The function detects wraparound conditions and handles them differently based on the server mode (normal postmaster vs. bootstrap/standalone).

**Range Reservation**: The function enforces different OID ranges for different purposes:
- During initdb: OIDs start from `FirstGenbkiObjectId`
- Normal operation: OIDs start from `FirstNormalObjectId`
- Reserved range: OIDs between these values are available for initdb automatic assignment

**Performance Optimization**: Uses a prefetch mechanism (`VAR_OID_PREFETCH`) to reduce WAL logging overhead by logging multiple OIDs at once and consuming them from a local counter.

**Recovery Protection**: Prevents OID allocation during recovery mode to maintain consistency.

## Parameters / Member Variables
This function takes no parameters and returns a newly allocated OID.

## Dependencies
- Functions called/Symbols referenced:
  - `RecoveryInProgress`
  - `LWLockAcquire` (OidGenLock, LW_EXCLUSIVE)
  - `XLogPutNextOid`
  - `LWLockRelease` (OidGenLock)
  - Constants: `FirstNormalObjectId`, `FirstGenbkiObjectId`, `VAR_OID_PREFETCH`
- Called from (representative examples):
  - `GetNewOidWithIndex` (src/backend/catalog/catalog.c:435, 450)
  - `GetNewRelFileNumber` (src/backend/catalog/catalog.c:580)

## Notes and Other Information
- **Not for direct use**: The function should generally not be called directly; instead use `GetNewOidWithIndex()` or `GetNewRelFileNumber()` which provide additional uniqueness guarantees
- **32-bit limitation**: OIDs will eventually wrap around, so uniqueness cannot be assumed without additional precautions
- **WAL logging**: Uses prefetch mechanism to log `VAR_OID_PREFETCH` OIDs at once, reducing WAL overhead
- **Bootstrap handling**: Behaves differently during initdb vs. normal operation to properly manage reserved OID ranges
- **Thread safety**: Protected by `OidGenLock` to ensure atomic allocation in multi-process environment
- **Recovery safety**: Explicitly prevents allocation during recovery mode to avoid conflicts with replay