# bbstreamer_recovery_injector_finalize

## Location
[src/bin/pg_basebackup/bbstreamer_inject.c:200-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_inject.c#L200-L208)

## Overview
Performs end-of-stream processing for the recovery injector bbstreamer by forwarding the finalization call to the next bbstreamer in the chain.

## Definition

```c
static void
bbstreamer_recovery_injector_finalize(bbstreamer *streamer)
```
## Detailed Description
This function implements the finalize operation for the bbstreamer_recovery_injector. It serves as a simple pass-through function that forwards the finalization call to the next bbstreamer in the processing chain. The function follows the standard bbstreamer pattern of delegating finalization responsibilities to subsequent streamers, ensuring proper cleanup and termination of the entire streaming pipeline.

This is a standard implementation for bbstreamer finalization that doesn't require any special cleanup logic specific to the recovery injector functionality.

## Parameters / Member Variables
- `*streamer`: The bbstreamer instance being finalized
## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_finalize](bbstreamer_finalize.md)
  - [bbstreamer](bbstreamer.md) (struct type)
- Called from (representative examples):
  - No direct references found (likely called via function pointer in operations table)

## Notes and Other Information
- Static function used as part of the bbstreamer_recovery_injector operations table
- Simple pass-through implementation that delegates to the next bbstreamer in the chain
- No special cleanup required for recovery injector state
- Part of the standard bbstreamer lifecycle management
- Located in src/bin/pg_basebackup/bbstreamer_inject.c:200-208

## Simplified Source

```c
static void
bbstreamer_recovery_injector_finalize(bbstreamer *streamer)
{
    // Pass finalization to next streamer in chain
    bbstreamer_finalize(streamer->bbs_next);
}
```