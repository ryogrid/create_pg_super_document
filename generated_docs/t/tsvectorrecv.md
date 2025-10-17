# tsvectorrecv

## Location
[src/backend/utils/adt/tsvector.c:446-554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L446-L554)

## Overview
The  function deserializes binary data received over the network into a TSVector data structure, performing validation and proper memory management during reconstruction.

## Definition

```c
struct, and copy lexeme.
		 *
		 * But make sure the buffer is large enough first.
		 */
		while (hdrlen + SHORTALIGN(datalen + lex_len) +
			   sizeof(uint16) + npos * sizeof(WordEntryPos) >= len)
		{
			len *= 2;
			vec = (TSVector) repalloc(vec, len);
		}

		vec->entries[i].haspos = (npos > 0) ? 1 : 0;
```
## Detailed Description
This function is the binary receive function for the TSVector data type, responsible for parsing binary data transmitted via PostgreSQL's binary protocol and reconstructing it into a valid TSVector structure. The function reads the binary format created by : starting with the lexeme count, then for each lexeme reading the null-terminated text, position count, and position data.

The function performs extensive validation during deserialization, checking lexeme lengths against MAXSTRLEN, total data size against MAXSTRPOS, and position counts against MAXNUMPOS. It dynamically allocates and reallocates memory as needed to accommodate the incoming data, ensuring proper alignment for position data structures. The function also maintains lexeme ordering by detecting when sorting is needed and applying qsort with compareentry function when necessary. Position data is validated to ensure positions are in ascending order within each lexeme.

## Parameters
- `fcinfo`: Standard PostgreSQL function argument macro containing the binary data buffer (StringInfo)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER: Extract StringInfo buffer from function arguments
  - [pq_getmsgint](../p/pq_getmsgint.md): Read integer from binary message buffer
  - [pq_getmsgstring](../p/pq_getmsgstring.md): Read null-terminated string from binary message buffer
  - [palloc0](../p/palloc0.md): Allocate zero-initialized memory
  - [repalloc](../r/repalloc.md): Reallocate memory with larger size
  - STRPTR: Get pointer to string data in TSVector
  - POSDATAPTR: Get pointer to position data
  - [compareentry](../c/compareentry.md): Compare function for WordEntry sorting
  - qsort_arg: Sort function with custom comparison
  - ARRPTR: Get pointer to word entries array
  - SHORTALIGN: Align to 2-byte boundary
  - SET_VARSIZE: Set variable-length data structure size
  - WEP_GETPOS: Extract position from WordEntryPos
  - PG_RETURN_TSVECTOR: Return TSVector result
- Called from (representative examples):
  - PostgreSQL binary protocol handlers
  - Client-server communication for TSVector data reception

## Notes and Other Information
- Performs comprehensive validation of all incoming data to prevent malformed TSVector creation
- Handles dynamic memory allocation with proper alignment requirements for position data
- Maintains lexeme ordering through conditional sorting when input is not pre-sorted
- Validates position sequences within each lexeme to ensure ascending order
- Uses zero-initialized memory allocation and careful padding for alignment requirements
- Maximum limits enforced: MAXSTRLEN for lexeme length, MAXSTRPOS for total length, MAXNUMPOS for position count
- Memory is reallocated as needed during parsing to accommodate variable-sized data

## Simplified Source

```c
Datum tsvectorrecv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);

    // Read number of lexemes
    int32 num_entries = pq_getmsgint(buf, sizeof(int32));
    if (num_entries < 0 || num_entries > (MaxAllocSize / sizeof(WordEntry))) {
        elog(ERROR, "invalid size of tsvector");
    }

    // Allocate initial TSVector structure
    Size header_len = DATAHDRSIZE + sizeof(WordEntry) * num_entries;
    Size total_len = header_len * 2;
    TSVector vec = (TSVector) palloc0(total_len);
    vec->size = num_entries;

    int data_len = 0;
    bool need_sort = false;

    // Process each lexeme
    for (int i = 0; i < num_entries; i++) {
        // Read lexeme text and position count
        const char *lexeme = pq_getmsgstring(buf);
        uint16 pos_count = (uint16) pq_getmsgint(buf, sizeof(uint16));

        // Validate input
        size_t lex_len = strlen(lexeme);
        if (lex_len > MAXSTRLEN || data_len > MAXSTRPOS || pos_count > MAXNUMPOS) {
            elog(ERROR, "invalid tsvector data");
        }

        // Ensure buffer is large enough
        while (header_len + SHORTALIGN(data_len + lex_len) +
               sizeof(uint16) + pos_count * sizeof(WordEntryPos) >= total_len) {
            total_len *= 2;
            vec = (TSVector) repalloc(vec, total_len);
        }

        // Fill word entry
        vec->entries[i].haspos = (pos_count > 0) ? 1 : 0;
        vec->entries[i].len = lex_len;
        vec->entries[i].pos = data_len;

        // Copy lexeme text
        memcpy(STRPTR(vec) + data_len, lexeme, lex_len);
        data_len += lex_len;

        // Check if sorting is needed
        if (i > 0 && compareentry(&vec->entries[i], &vec->entries[i - 1], STRPTR(vec)) <= 0) {
            need_sort = true;
        }

        // Read position data if present
        if (pos_count > 0) {
            // Align data for position storage
            if (data_len != SHORTALIGN(data_len)) {
                *(STRPTR(vec) + data_len) = '\0';
                data_len = SHORTALIGN(data_len);
            }

            // Store position count
            memcpy(STRPTR(vec) + data_len, &pos_count, sizeof(uint16));

            // Read position values
            WordEntryPos *positions = POSDATAPTR(vec, &vec->entries[i]);
            for (int j = 0; j < pos_count; j++) {
                positions[j] = (WordEntryPos) pq_getmsgint(buf, sizeof(WordEntryPos));
                if (j > 0 && WEP_GETPOS(positions[j]) <= WEP_GETPOS(positions[j - 1])) {
                    elog(ERROR, "position information is misordered");
                }
            }

            data_len += sizeof(uint16) + pos_count * sizeof(WordEntryPos);
        }
    }

    SET_VARSIZE(vec, header_len + data_len);

    // Sort if necessary
    if (need_sort) {
        qsort_arg(ARRPTR(vec), vec->size, sizeof(WordEntry), compareentry, STRPTR(vec));
    }

    PG_RETURN_TSVECTOR(vec);
}
```