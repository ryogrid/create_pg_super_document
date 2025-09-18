# MultiXactSetNextMXact

## Location
src/backend/access/transam/multixact.c: 2320 - 2353

## Overview
Sets the next MultiXact ID and offset values to be assigned, typically used during bootstrap and WAL replay operations.

## Definition


## Detailed Description
MultiXactSetNextMXact is responsible for setting the next MultiXact ID and corresponding offset that will be assigned when new MultiXacts are created. This function is primarily used during system bootstrap and WAL replay operations where the exact next values can be determined from checkpoint records or other authoritative sources.

The function operates under exclusive lock protection to ensure atomic updates to the shared MultiXact state, even though it's typically called during bootstrap and replay when concurrent access is limited. This locking is maintained for safety in case hot-standby backends are examining these values.

A special consideration is made for binary upgrade operations, where the function ensures that the offsets SLRU (Simple Least Recently Used buffer) is large enough to contain the next value that would be created. This extension must happen early in the startup process, specifically before StartupMultiXact() but after the initial determination of nextMXact value.

## Parameters / Member Variables
- : The next MultiXact ID to be assigned
- : The corresponding offset for storing MultiXact member data

## Dependencies
- Functions called/Symbols referenced:
  - debug_elog4
  - LWLockAcquire
  - LWLockRelease
  - [MaybeExtendOffsetSlru](MaybeExtendOffsetSlru.md)
- Called from (representative examples):
  - [BootStrapXLOG](../B/BootStrapXLOG.md)
  - [StartupXLOG](../S/StartupXLOG.md)
  - [xlog_redo](../x/xlog_redo.md)

## Notes and Other Information
- Primarily used during bootstrap and WAL replay operations
- Takes exclusive lock on MultiXactGenLock for thread safety with hot-standby backends
- Includes debug logging to track MultiXact state changes
- During binary upgrades, automatically extends the offsets SLRU if necessary
- Must be called early in startup sequence, before StartupMultiXact()
- Critical for maintaining correct MultiXact ID sequencing during recovery
- The IsBinaryUpgrade check ensures proper SLRU sizing during database upgrades