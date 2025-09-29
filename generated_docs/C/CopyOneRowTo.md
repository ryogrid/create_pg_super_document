# CopyOneRowTo

## Location
[src/backend/commands/copyto.c:907-979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L907-L979)

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
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [CopySendInt16](CopySendInt16.md)
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [CopySendChar](CopySendChar.md)
  - [CopySendString](CopySendString.md)
  - [CopySendInt32](CopySendInt32.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [CopyAttributeOutCSV](CopyAttributeOutCSV.md)
  - [CopyAttributeOutText](CopyAttributeOutText.md)
  - [SendFunctionCall](../S/SendFunctionCall.md)
  - [CopySendData](CopySendData.md)
  - [CopySendEndOfRow](CopySendEndOfRow.md)
- Called from (representative examples):
  - [DoCopyTo](../D/DoCopyTo.md)
  - [copy_dest_receive](../c/copy_dest_receive.md)
  - DR_copy

## Notes and Other Information
The function distinguishes between binary and text formats, with binary format requiring length prefixes for each attribute and special encoding for NULL values (-1 length). Text format uses configurable delimiters and NULL representation strings. The function uses the appropriate output functions (text or binary) that were pre-configured during BeginCopyTo setup. Memory management is handled carefully by operating within the row context and resetting it at each call to prevent accumulation of temporary data across rows. The function handles CSV mode specially when in text format, applying proper quoting and escaping rules.

## Simplified Source

```c
static void CopyOneRowTo(CopyToState cstate, TupleTableSlot *slot)
{
    bool need_delim = false;
    FmgrInfo *out_functions = cstate->out_functions;
    MemoryContext oldcontext;
    ListCell *cur;
    char *string;

    // Reset memory context for this row
    MemoryContextReset(cstate->rowcontext);
    oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

    if (cstate->opts.binary)
    {
        // Binary format: send column count header
        CopySendInt16(cstate, list_length(cstate->attnumlist));
    }

    // Extract all attributes from the slot
    slot_getallattrs(slot);

    // Process each attribute in the column list
    foreach(cur, cstate->attnumlist)
    {
        int attnum = lfirst_int(cur);
        Datum value = slot->tts_values[attnum - 1];
        bool isnull = slot->tts_isnull[attnum - 1];

        // Add delimiter for text format (except first column)
        if (!cstate->opts.binary)
        {
            if (need_delim)
                CopySendChar(cstate, cstate->opts.delim[0]);
            need_delim = true;
        }

        if (isnull)
        {
            // Handle NULL values
            if (!cstate->opts.binary)
                CopySendString(cstate, cstate->opts.null_print_client);
            else
                CopySendInt32(cstate, -1);  // Binary NULL marker
        }
        else
        {
            if (!cstate->opts.binary)
            {
                // Text format: convert value to string
                string = OutputFunctionCall(&out_functions[attnum - 1], value);
                if (cstate->opts.csv_mode)
                    CopyAttributeOutCSV(cstate, string,
                                      cstate->opts.force_quote_flags[attnum - 1]);
                else
                    CopyAttributeOutText(cstate, string);
            }
            else
            {
                // Binary format: send length-prefixed binary data
                bytea *outputbytes;
                outputbytes = SendFunctionCall(&out_functions[attnum - 1], value);
                CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
                CopySendData(cstate, VARDATA(outputbytes),
                           VARSIZE(outputbytes) - VARHDRSZ);
            }
        }
    }

    // Send end-of-row marker
    CopySendEndOfRow(cstate);

    // Restore previous memory context
    MemoryContextSwitchTo(oldcontext);
}
```