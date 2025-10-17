# tsvectorout

## Location
[src/backend/utils/adt/tsvector.c:314-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L314-L406)

## Overview
The  function converts a PostgreSQL TSVector data type to its textual string representation for output and display purposes.

## Definition

```c
Datum
tsvectorout(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the output function for the TSVector data type, which serializes a TSVector's internal binary representation into a human-readable string format. The function handles the complex task of formatting lexemes along with their positional information and weights. Each lexeme is enclosed in single quotes and properly escaped, with positions and weights (A, B, C, D) displayed after a colon when present. The output format follows the pattern: .

The function carefully calculates the required buffer size to accommodate all lexemes, their positions, weights, and necessary escape characters. It processes each word entry in the TSVector, properly escaping single quotes and backslashes within lexeme text, and formats positional data with corresponding weight letters (A=weight 3, B=weight 2, C=weight 1, D or no letter=weight 0).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the TSVector input parameter
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extract TSVector from function arguments
  - ARRPTR: Get pointer to word entries array
  - STRPTR: Get pointer to string data
  - POSDATAPTR: Get pointer to position data
  - POSDATALEN: Get length of position data
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md): Get maximum character length for encoding
  - [pg_mblen](../p/pg_mblen.md): Get multibyte character length
  - t_iseq: Test character equality
  - WEP_GETPOS: Extract position from word entry position data
  - WEP_GETWEIGHT: Extract weight from word entry position data
  - PG_RETURN_CSTRING: Return C string result
- Called from (representative examples):
  - PostgreSQL type system for TSVector output operations
  - SQL queries requiring TSVector text representation

## Notes and Other Information
- The function properly handles multibyte character encodings
- Memory allocation is carefully calculated to prevent buffer overruns
- Single quotes and backslashes within lexemes are properly escaped by doubling them
- Position weights are represented as letters: A (highest), B, C, D (or omitted for lowest)
- The output format is compatible with tsvector input parsing functions

## Simplified Source

```c
Datum tsvectorout(PG_FUNCTION_ARGS) {
    TSVector tsvector = PG_GETARG_TSVECTOR(0);
    WordEntry *entries = ARRPTR(tsvector);

    // Calculate output buffer size
    int buffer_size = tsvector->size * 2 + tsvector->size - 1 + 2;
    for (int i = 0; i < tsvector->size; i++) {
        buffer_size += entries[i].len * 2 * pg_database_encoding_max_length();
        if (entries[i].haspos) {
            buffer_size += 1 + 7 * POSDATALEN(tsvector, &entries[i]);
        }
    }

    // Build output string
    char *output = palloc(buffer_size);
    char *current = output;

    for (int i = 0; i < tsvector->size; i++) {
        char *lexeme_start = STRPTR(tsvector) + entries[i].pos;

        // Add separator and opening quote
        if (i != 0) *current++ = ' ';
        *current++ = '\'';

        // Copy lexeme with escape handling
        for (int j = 0; j < entries[i].len; j++) {
            if (lexeme_start[j] == '\'' || lexeme_start[j] == '\\') {
                *current++ = lexeme_start[j]; // Escape by doubling
            }
            *current++ = lexeme_start[j];
        }

        *current++ = '\'';

        // Add position data if present
        int pos_count = POSDATALEN(tsvector, &entries[i]);
        if (pos_count > 0) {
            *current++ = ':';
            WordEntryPos *positions = POSDATAPTR(tsvector, &entries[i]);

            for (int p = 0; p < pos_count; p++) {
                current += sprintf(current, "%d", WEP_GETPOS(positions[p]));

                // Add weight letter (A=3, B=2, C=1, D=0/default)
                switch (WEP_GETWEIGHT(positions[p])) {
                    case 3: *current++ = 'A'; break;
                    case 2: *current++ = 'B'; break;
                    case 1: *current++ = 'C'; break;
                    default: break; // No letter for weight 0
                }

                if (p < pos_count - 1) *current++ = ',';
            }
        }
    }

    *current = '\0';
    PG_RETURN_CSTRING(output);
}
```