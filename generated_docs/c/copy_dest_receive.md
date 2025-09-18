# copy_dest_receive

## Location
src/backend/commands/copyto.c: 1235 - 1253

## Overview
Processes individual tuples received by the COPY destination receiver, formatting and outputting each row while tracking progress statistics.

## Definition


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