# pqTraceOutput_CopyOutResponse

## Location
src/interfaces/libpq/fe-trace.c: 386 - 398

## Overview
Outputs formatted trace information for PostgreSQL CopyOutResponse messages, displaying the copy format and column format specifications for COPY operations from server to client.

## Definition


## Detailed Description
This function parses and outputs trace information for CopyOutResponse messages in the PostgreSQL frontend protocol. CopyOutResponse messages are sent by the server to indicate that it is ready to send COPY data to the client. The function extracts and displays the overall format of the copy operation (text or binary) and the format codes for each column being copied.

The message format includes:
1. A single byte indicating the overall copy format (0 = text, 1 = binary)
2. The number of columns in the copy operation
3. Format codes for each column (0 = text, 1 = binary)

## Parameters / Member Variables
- : Output file stream where the formatted trace information will be written
- : Pointer to the raw binary message data containing the CopyOutResponse information
- 
  ╭──────────────────────────────────────────────────────────────────────────╮
  │                                                                          │
  │  ℹ Choose the default behavior for 'cursor'                              │
  │                                                                          │
  │  What should happen when you run 'cursor' with no arguments?             │
  │  You can still do `cursor .` to open Cursor in your folder.              │
  │                                                                          │
  │                                                                          │
  │  ▶ [a] Start Cursor Agent (chat in terminal)                             │
  │    [c] Open Cursor IDE                                                   │
  │                                                                          │
  │  Use arrow keys to navigate, Enter to select, or press the key shown     │
  │                                                                          │
  ╰──────────────────────────────────────────────────────────────────────────╯: Pointer to current position in the message buffer, updated as data is read

## Dependencies
- Functions called/Symbols referenced:
  - pqTraceOutputByte1 (for overall copy format)
  - pqTraceOutputInt16 (for number of fields and individual format codes)
- Called from (representative examples):
  - pqTraceOutputMessage

## Notes and Other Information
- This is a static function internal to the fe-trace.c module
- CopyOutResponse indicates the server is ready to send COPY data to the client
- The trace output format begins with "CopyOutResponse" followed by the parsed message components
- Format codes: 0 indicates text format, 1 indicates binary format
- This message type is part of the COPY protocol flow in PostgreSQL
- The function properly handles variable numbers of columns by reading the count first
- Structurally identical to pqTraceOutput_CopyInResponse but handles the opposite direction of data flow