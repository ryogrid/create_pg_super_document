# do_text_output_multiline

## Location
[src/backend/executor/execTuples.c:2390-2419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2390-L2419)

## Overview
Writes a chunk of text as multiple tuple rows, breaking the text at newline characters and outputting each line as a separate tuple with a single TEXT attribute.

## Definition
```c
void do_text_output_multiline(TupOutputState *tstate, const char *txt)
```

## Detailed Description
do_text_output_multiline is a specialized function for outputting large text content that spans multiple lines. It processes a text string by splitting it at newline characters and sending each line as a separate tuple through the tuple output infrastructure.

The function operates by:
1. Iterating through the input text character by character
2. Finding newline characters using strchr() to identify line boundaries
3. For each line segment, converting it to a PostgreSQL text datum using cstring_to_text_with_len()
4. Calling do_tup_output() to send the line as a single-column tuple
5. Properly managing memory by freeing each text datum after output
6. Continuing until the entire input text is processed

This function is particularly useful for commands like EXPLAIN that need to output multi-line formatted text as structured query results.

## Parameters / Member Variables
- `tstate`: TupOutputState pointer containing the tuple output infrastructure, which must be configured with a single TEXT attribute tuple descriptor
- `txt`: C string containing the text to be output, which will be split at newline characters for multi-row output

## Dependencies
- Functions called/Symbols referenced:
  - strchr
  - strlen
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - [do_tup_output](do_tup_output.md)
  - [pfree](../p/pfree.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - [ExplainQuery](../E/ExplainQuery.md) (explain.c)

## Notes and Other Information
- Designed specifically for use with single-TEXT-attribute tuple descriptors
- Each line (text segment between newlines) becomes a separate row in the output
- Properly handles text conversion and memory management for PostgreSQL text datums
- The function processes the entire text in a single call, making it efficient for large text blocks
- Empty lines (consecutive newlines) will result in empty string tuples in the output
- Used primarily by the EXPLAIN command to output query plans as structured results
- Memory management is handled correctly with pfree() calls to prevent leaks

## Simplified Source

```c
void do_text_output_multiline(TupOutputState *tstate, const char *txt) {
    Datum values[1];
    bool isnull[1] = {false};

    // Process text line by line, splitting at newlines
    while (*txt) {
        const char *eol;
        int len;

        // Find next newline or end of string
        eol = strchr(txt, '\n');
        if (eol) {
            len = eol - txt;  // Length of current line
            eol++;            // Move past newline
        } else {
            len = strlen(txt); // Length to end of string
            eol = txt + len;   // Point to end
        }

        // Convert line to PostgreSQL text datum and output as tuple
        values[0] = PointerGetDatum(cstring_to_text_with_len(txt, len));
        do_tup_output(tstate, values, isnull);

        // Clean up memory and advance to next line
        pfree(DatumGetPointer(values[0]));
        txt = eol;
    }
}
```