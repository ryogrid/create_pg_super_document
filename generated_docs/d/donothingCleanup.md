# donothingCleanup

## Location
[src/backend/tcop/dest.c:61-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/dest.c#L61-L102)

## Overview
donothingCleanup is a dummy DestReceiver cleanup function that performs no cleanup operations, serving as a no-operation placeholder for both shutdown and destroy methods.

## Definition
static void donothingCleanup(DestReceiver *self)

## Detailed Description
This function is part of PostgreSQL's destination receiver infrastructure and serves as a placeholder implementation for cleanup operations when no teardown is required. It's designed to be used in contexts where DestReceiver cleanup callbacks are mandatory but no actual cleanup processing should occur. The function has an empty body and is explicitly documented to be used for both shutdown and destroy methods of destination receivers.

## Parameters / Member Variables
- self: DestReceiver pointer to the destination receiver object (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [DestReceiver](../D/DestReceiver.md) (type reference)
- Called from (representative examples):
  - donothingDR (static DestReceiver struct for DestNone)
  - debugtupDR (static DestReceiver struct for DestDebug)
  - printsimpleDR (static DestReceiver struct for DestRemoteSimple)
  - spi_printtupDR (static DestReceiver struct for DestSPI)

## Notes and Other Information
- This is a static function, limiting its scope to the dest.c file
- Part of the dummy DestReceiver functions suite alongside donothingReceive and donothingStartup
- Used for both shutdown and destroy method implementations as noted in the comment
- Commonly reused across multiple DestReceiver struct definitions that do not require cleanup
- Has an empty function body, performing no actual cleanup operations

## Simplified Source

```c
static void donothingCleanup(DestReceiver *self)
{
    // No-op cleanup function for DestReceiver objects
    // Used when no cleanup is required
}
```