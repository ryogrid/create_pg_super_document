# printtup

## Location
src/backend/access/common/printtup.c: 304 - 388

## Overview
The printtup function sends a tuple (row) to the client through the PostgreSQL communication protocol, handling both text and binary output formats.

## Definition


## Detailed Description
The printtup function is a core component of PostgreSQL's result delivery system. It takes a tuple stored in a TupleTableSlot and formats it for transmission to the client according to the PostgreSQL wire protocol. The function supports both text and binary output formats and handles NULL values appropriately.

The function operates by:
1. Extracting tuple descriptor information and preparing attribute info if needed
2. Fully deconstructing the tuple to access all attributes
3. Creating a DataRow message using the PostgreSQL message protocol
4. Iterating through all attributes and formatting each according to its type and requested format
5. Sending the complete message to the client

The function includes memory management by switching to a temporary memory context during processing and resetting it after each row to prevent memory leaks.

## Parameters / Member Variables
- : TupleTableSlot containing the tuple data to be sent to the client
- : DestReceiver pointer that contains state information for the print operation, cast to DR_printtup type

## Dependencies
- Functions called/Symbols referenced:
  - [printtup_prepare_info](printtup_prepare_info.md): Prepares attribute information for output formatting
  - slot_getallattrs: Ensures all attributes in the slot are deconstructed
  - pq_beginmessage_reuse: Starts a PostgreSQL protocol message
  - [pq_sendint16](pq_sendint16.md): Sends a 16-bit integer in network byte order
  - [pq_sendint32](pq_sendint32.md): Sends a 32-bit integer in network byte order
  - pq_sendcountedtext: Sends text data with length prefix
  - pq_sendbytes: Sends binary data
  - [pq_endmessage_reuse](pq_endmessage_reuse.md): Completes and sends the protocol message
  - [OutputFunctionCall](../O/OutputFunctionCall.md): Converts datum to text representation
  - [SendFunctionCall](../S/SendFunctionCall.md): Converts datum to binary representation
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md): Switches memory contexts
  - [MemoryContextReset](../M/MemoryContextReset.md): Resets temporary memory context
- Called from (representative examples):
  - [printtup_create_DR](printtup_create_DR.md): Creates a DestReceiver that uses printtup

## Notes and Other Information
- The function is marked as static, indicating it's only used within the printtup.c file
- Includes Valgrind memory checking for debugging undefined bytes in variable-length datums
- The comment references serializeAnalyzeReceive in explain.c which replicates similar computations
- Memory management is carefully handled using temporary contexts to avoid memory leaks during row processing
- Supports both text (format 0) and binary (format 1) output formats for attributes