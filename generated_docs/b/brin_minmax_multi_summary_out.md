# brin_minmax_multi_summary_out

## Location
[src/backend/access/brin/brin_minmax_multi.c:2998-3116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2998-L3116)

## Overview
This function serves as the output routine for the BRIN minmax-multi summary type, converting the binary serialized summary data into a human-readable string format for display purposes.

## Definition
```c
Datum brin_minmax_multi_summary_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `brin_minmax_multi_summary_out` function is responsible for converting BRIN minmax-multi summaries from their internal bytea serialized format into a readable string representation. BRIN (Block Range INdex) minmax-multi summaries store multiple value ranges and individual values to efficiently index data blocks.

The function deserializes the input data, extracts range and value information, and formats it into a structured string showing:
- Number of ranges, values, and maximum values
- Individual ranges formatted as "min ... max"
- Individual values as an array

The output format follows the pattern: `{nranges: N nvalues: M maxvalues: K ranges: [...] values: [...]}`

## Parameters / Member Variables
This function follows the PostgreSQL function calling convention using `PG_FUNCTION_ARGS`:
- Input parameter (accessed via `PG_GETARG_DATUM(0)`): A serialized BRIN minmax-multi summary stored as bytea

## Dependencies
- Functions called/Symbols referenced:
  - [SerializedRanges](../S/SerializedRanges.md) (struct type)
  - [Ranges](../R/Ranges.md) (struct type)
  - PG_DETOAST_DATUM
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [brin_range_deserialize](brin_range_deserialize.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - [accumArrayResult](../a/accumArrayResult.md)
  - [makeArrayResult](../m/makeArrayResult.md)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - FunctionCall1
  - [cstring_to_text](../c/cstring_to_text.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - PG_RETURN_CSTRING
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's type system)

## Notes and Other Information
- This function is part of PostgreSQL's BRIN index infrastructure for the minmax-multi operator class
- The function handles both range data (min/max pairs) and individual values
- Memory management is handled through PostgreSQL's memory context system
- The output is intended for human consumption rather than machine processing
- Located in src/backend/access/brin/brin_minmax_multi.c:2998-3116

## Simplified Source

```c
Datum brin_minmax_multi_summary_out(PG_FUNCTION_ARGS) {
    SerializedRanges *ranges;
    Ranges *ranges_deserialized;
    StringInfoData str;
    FmgrInfo fmgrinfo;
    ArrayBuildState *astate_values = NULL;

    // Initialize output string
    initStringInfo(&str);
    appendStringInfoChar(&str, '{');

    // Detoast and deserialize the input summary
    ranges = (SerializedRanges *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ranges_deserialized = brin_range_deserialize(ranges->maxvalues, ranges);

    // Get output function for the data type
    Oid outfunc;
    bool isvarlena;
    getTypeOutputInfo(ranges->typid, &outfunc, &isvarlena);
    fmgr_info(outfunc, &fmgrinfo);

    // Output summary statistics
    appendStringInfo(&str, "nranges: %d  nvalues: %d  maxvalues: %d",
                     ranges_deserialized->nranges,
                     ranges_deserialized->nvalues,
                     ranges_deserialized->maxvalues);

    // Format and output ranges as "min ... max"
    int idx = 0;
    for (int i = 0; i < ranges_deserialized->nranges; i++) {
        char *min_val = OutputFunctionCall(&fmgrinfo, ranges_deserialized->values[idx++]);
        char *max_val = OutputFunctionCall(&fmgrinfo, ranges_deserialized->values[idx++]);

        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "%s ... %s", min_val, max_val);

        text *range_text = cstring_to_text_with_len(buf.data, buf.len);
        astate_values = accumArrayResult(astate_values, PointerGetDatum(range_text),
                                        false, TEXTOID, CurrentMemoryContext);
    }

    // Output ranges array if present
    if (ranges_deserialized->nranges > 0) {
        Datum ranges_array = makeArrayResult(astate_values, CurrentMemoryContext);
        char *ranges_str = OidOutputFunctionCall(ANYARRAYOID, ranges_array);
        appendStringInfo(&str, " ranges: %s", ranges_str);
    }

    // Format and output individual values
    astate_values = NULL;
    for (int i = 0; i < ranges_deserialized->nvalues; i++) {
        Datum val_str = FunctionCall1(&fmgrinfo, ranges_deserialized->values[idx++]);
        text *val_text = cstring_to_text(DatumGetCString(val_str));
        astate_values = accumArrayResult(astate_values, PointerGetDatum(val_text),
                                        false, TEXTOID, CurrentMemoryContext);
    }

    // Output values array if present
    if (ranges_deserialized->nvalues > 0) {
        Datum values_array = makeArrayResult(astate_values, CurrentMemoryContext);
        char *values_str = OidOutputFunctionCall(ANYARRAYOID, values_array);
        appendStringInfo(&str, " values: %s", values_str);
    }

    appendStringInfoChar(&str, '}');
    PG_RETURN_CSTRING(str.data);
}
```