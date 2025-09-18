# CopyOneRowTo

## Location
src/backend/commands/copyto.c: 907 - 979

## Overview
CopyOneRowTo formats and outputs a single tuple from a TupleTableSlot according to the configured COPY TO format settings, handling both binary and text output modes.

## Definition
```c
static void CopyOneRowTo(CopyToState cstate, TupleTableSlot *slot)
```

## Detailed Description
CopyOneRowTo processes individual tuples during COPY TO operations by extracting attribute values from the provided slot and formatting them according to the copy state configuration. The function handles both binary and text formats, manages NULL value representation, applies appropriate delimiters for text format, and uses the configured output functions for data conversion. It operates within the row memory context to ensure efficient memory management, resetting the context at the start of each row and properly cleaning up afterward.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing formatting configuration and output functions
- `slot`: TupleTableSlot containing the tuple data to be formatted and output

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextReset
  - CopySendInt16
  - slot_getallattrs
  - CopySendChar
  - CopySendString
  - CopySendInt32
  - OutputFunctionCall
  - CopyAttributeOutCSV
  - CopyAttributeOutText
  - SendFunctionCall
  - CopySendData
  - CopySendEndOfRow
- Called from (representative examples):
  - DoCopyTo
  - copy_dest_receive
  - DR_copy

## Notes and Other Information
The function distinguishes between binary and text formats, with binary format requiring length prefixes for each attribute and special encoding for NULL values (-1 length). Text format uses configurable delimiters and NULL representation strings. The function uses the appropriate output functions (text or binary) that were pre-configured during BeginCopyTo setup. Memory management is handled carefully by operating within the row context and resetting it at each call to prevent accumulation of temporary data across rows. The function handles CSV mode specially when in text format, applying proper quoting and escaping rules.