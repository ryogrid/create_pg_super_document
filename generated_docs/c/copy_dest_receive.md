# copy_dest_receive

## Location
[src/backend/commands/copyto.c:1235-1253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L1235-L1253)

## Overview
Processes individual tuples received by the COPY destination receiver, formatting and outputting each row while tracking progress statistics.

## Definition

```c
static bool
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
```
## Detailed Description
This function serves as the main tuple processing callback for COPY TO operations within PostgreSQL's executor framework. It receives individual tuples from the executor, extracts the COPY state information from the destination receiver, and delegates the actual row formatting and output to CopyOneRowTo. Additionally, it maintains progress tracking by incrementing the processed tuple counter and reporting progress updates through the statistics system. The function always returns true to indicate successful processing and continuation of the operation.

## Parameters / Member Variables
- : TupleTableSlot containing the tuple data to be processed and output
- : DestReceiver structure cast to DR_copy containing COPY operation state and configuration

## Dependencies
- Functions called/Symbols referenced:
  -  (cast type for the destination receiver)
  -  (state structure type)
  -  (function to format and output a single row)
  -  (progress reporting function)
  -  (progress parameter constant)
- Called from (representative examples):
  -  (during receiver setup as callback assignment)

## Notes and Other Information
- This is a callback function that gets assigned to the DestReceiver's receiveSlot field during COPY destination receiver initialization
- The function integrates with PostgreSQL's progress reporting system to provide real-time feedback on COPY operation status
- Always returns true, indicating that COPY operations don't implement early termination at the tuple level
- Part of the executor's destination receiver framework that allows pluggable output destinations

## Simplified Source

```c
// Simplified version of copy_dest_receive
static bool
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
    // Extract COPY state from destination receiver
    DR_copy *myState = (DR_copy *) self;
    CopyToState cstate = myState->cstate;

    // Core logic: Format and send the tuple data
    CopyOneRowTo(cstate, slot);

    // Track progress: Increment processed tuple count
    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
                                ++myState->processed);

    // Always return true to continue processing
    return true;
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Preserved all essential functionality - no logic was removed
- Enhanced readability through better comment structure
- Maintained the exact same algorithm flow
- Function is already quite simple and focused, so minimal changes were needed