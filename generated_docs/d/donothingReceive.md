# donothingReceive

## Location
[src/backend/tcop/dest.c:50-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/dest.c#L50-L55)

## Overview
donothingReceive is a dummy DestReceiver function that serves as a no-operation tuple receiver, always returning true without processing the tuple data.

## Definition
static bool donothingReceive(TupleTableSlot *slot, DestReceiver *self)

## Detailed Description
This function is part of PostgreSQL's destination receiver infrastructure and serves as a placeholder implementation for scenarios where tuple processing is not required. It's designed to be used in contexts where a DestReceiver callback is mandatory but no actual tuple processing should occur. The function simply returns true to indicate successful "processing" without performing any operations on the tuple data.

## Parameters / Member Variables
- slot: TupleTableSlot pointer containing the tuple data to be "processed" (ignored in this implementation)
- self: DestReceiver pointer to the destination receiver object (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [DestReceiver](../D/DestReceiver.md) (type reference)
- Called from (representative examples):
  - Used indirectly through DestReceiver function pointer assignments

## Notes and Other Information
- This is a static function, limiting its scope to the dest.c file
- Part of the dummy DestReceiver functions suite alongside donothingStartup and donothingCleanup
- Returns true unconditionally, indicating successful tuple handling to the caller
- Commonly used in testing scenarios or when tuple output needs to be discarded