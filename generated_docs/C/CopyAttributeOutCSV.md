# CopyAttributeOutCSV

## Location
[src/backend/commands/copyto.c:1140-1225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L1140-L1225)

## Overview
Outputs a single attribute value in CSV format with proper escaping and quoting as needed for PostgreSQL's COPY TO command.

## Definition

```c
static void
CopyAttributeOutCSV(CopyToState cstate, const char *string,
					bool use_quote)
```
## Detailed Description
This function handles the CSV formatting of individual attribute values during COPY TO operations. It performs character encoding conversion if needed, determines whether the value requires quoting based on CSV rules, and applies appropriate escaping for special characters. The function implements PostgreSQL's CSV output format which includes:

- Automatic quoting detection for values containing delimiters, quotes, or newlines
- Special handling for the end-of-data marker "\\" when it appears as a single attribute
- Proper escaping of quote and escape characters within quoted values
- Character encoding conversion when transcoding is required
- Null value detection to force quoting when a value matches the null representation

## Parameters / Member Variables
- : CopyToState structure containing COPY operation configuration including delimiters, quote characters, escape characters, and encoding information
- : The attribute value to be formatted as a null-terminated string
- : Boolean flag indicating whether the caller has already determined that quoting is required

## Dependencies
- Functions called/Symbols referenced:
  -  (for checking single attribute case)
  -  (for null print comparison and end-of-data marker detection)
  -  (for character encoding conversion)
  -  (for string length calculation)
  -  (macro for detecting multi-byte characters)
  -  (for multi-byte character length calculation)
  -  (for sending individual characters to output)
  -  (for sending complete strings to output)
  -  (macro for efficient string segment output)
- Called from (representative examples):
  -  (copy destination receive function)
  -  (main COPY TO processing)
  -  (single row processing)

## Notes and Other Information
- Uses an optimization strategy similar to CopyAttributeOutText for efficient string processing by batching character output
- Handles multi-byte character encodings properly by using encoding-aware character length functions
- Forces quoting for values that match the null representation to avoid ambiguity
- Special case handling for the PostgreSQL end-of-data marker "\\" when it's the only attribute to prevent misinterpretation

## Simplified Source

```c
static void
CopyAttributeOutCSV(CopyToState cstate, const char *string,
                    bool use_quote)
{
    const char *ptr;
    const char *start;
    char c;
    char delimc = cstate->opts.delim[0];
    char quotec = cstate->opts.quote[0];
    char escapec = cstate->opts.escape[0];
    bool single_attr = (list_length(cstate->attnumlist) == 1);

    // Force quoting if it matches null_print (before conversion!)
    if (!use_quote && strcmp(string, cstate->opts.null_print) == 0)
        use_quote = true;

    if (cstate->need_transcoding)
        ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
    else
        ptr = string;

    // Make a preliminary pass to discover if it needs quoting
    if (!use_quote)
    {
        // Because '\\.' can be a data value, quote it if it appears alone
        if (single_attr && strcmp(ptr, "\\.") == 0)
            use_quote = true;
        else
        {
            const char *tptr = ptr;
            while ((c = *tptr) != '\0')
            {
                if (c == delimc || c == quotec || c == '\n' || c == '\r')
                {
                    use_quote = true;
                    break;
                }
                if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
                    tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
                else
                    tptr++;
            }
        }
    }

    if (use_quote)
    {
        CopySendChar(cstate, quotec);

        // Use the same optimization strategy as in CopyAttributeOutText
        start = ptr;
        while ((c = *ptr) != '\0')
        {
            if (c == quotec || c == escapec)
            {
                DUMPSOFAR();
                CopySendChar(cstate, escapec);
                start = ptr;    // we include char in next run
            }
            if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
                ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
            else
                ptr++;
        }
        DUMPSOFAR();

        CopySendChar(cstate, quotec);
    }
    else
    {
        // If it doesn't need quoting, we can just dump it as-is
        CopySendString(cstate, ptr);
    }
}
```