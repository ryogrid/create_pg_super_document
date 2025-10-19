# tsvector_concat

## Location
[src/backend/utils/adt/tsvector_op.c:925-1151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L925-L1151)

## Overview
Concatenates two tsvectors by merging their lexemes in sorted order, combining duplicate entries and adjusting position offsets to maintain proper position sequencing across the concatenated result.

## Definition

```c
Datum
tsvector_concat(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the concatenation operation for PostgreSQL tsvectors (typically used via the  operator). It performs a sorted merge of two input tsvectors, creating a new tsvector that contains all unique lexemes from both inputs. When duplicate lexemes are found, their positional information is merged. 

The function maintains proper position sequencing by finding the maximum position in the first tsvector and using it as an offset when adding positions from the second tsvector. This ensures that positions from the second tsvector appear after those from the first, preserving the logical document order.

The implementation uses a three-way merge algorithm similar to merging sorted arrays, handling cases where lexemes appear in only the first tsvector, only the second, or in both. Memory allocation is conservative initially, then compacted at the end to minimize space usage.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function argument structure containing:
  - Argument 0: First tsvector input
  - Argument 1: Second tsvector input

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extract tsvector arguments from function call
  - ARRPTR: Get pointer to WordEntry array in tsvector
  - STRPTR: Get pointer to string data in tsvector
  - POSDATALEN: Get length of position data for a word entry
  - POSDATAPTR: Get pointer to position data for a word entry
  - WEP_GETPOS: Extract position from WordEntryPos
  - VARSIZE: Get variable-length type size
  - SET_VARSIZE: Set variable-length type size
  - compareEntry: Compare two word entries lexicographically
  - _POSVECPTR: Get pointer to position vector for a word entry
  - SHORTALIGN: Align memory addresses to short boundaries
  - [add_pos](../a/add_pos.md): Helper function to add positions with offset
  - MAXSTRPOS: Maximum allowed string position
  - CALCDATASIZE: Calculate total data size for tsvector
  - PG_RETURN_POINTER: Return result pointer
  - PG_FREE_IF_COPY: Free copied input arguments

- Called from (representative examples):
  - No direct callers found (exposed as PostgreSQL SQL operator/function)

## Notes and Other Information
- The function implements the PostgreSQL  concatenation operator for tsvectors
- Position offsets are calculated to maintain proper document order across concatenation
- Lexemes are kept in sorted order in the result tsvector
- When duplicate lexemes exist, their positions are merged (union of positions)
- Memory is initially over-allocated conservatively, then compacted to actual size
- Position overflow checking ensures results don't exceed MAXSTRPOS limits
- The  helper function handles position offset arithmetic and overflow detection
- Handles cases where either input tsvector may be empty
- Maintains the haspos flags correctly for lexemes with and without positional data
- Uses SHORTALIGN for proper memory alignment of position data structures

## Simplified Source

```c
Datum tsvector_concat(PG_FUNCTION_ARGS) {
    TSVector in1 = PG_GETARG_TSVECTOR(0);
    TSVector in2 = PG_GETARG_TSVECTOR(1);
    TSVector out;
    WordEntry *ptr, *ptr1, *ptr2;
    int maxpos = 0, i, j, i1, i2, dataoff, output_bytes;
    char *data, *data1, *data2;

    // Find maximum position in first TSVector for offset calculation
    ptr = ARRPTR(in1);
    i = in1->size;
    while (i--) {
        if ((j = POSDATALEN(in1, ptr)) != 0) {
            WordEntryPos *p = POSDATAPTR(in1, ptr);
            while (j--) {
                if (WEP_GETPOS(*p) > maxpos)
                    maxpos = WEP_GETPOS(*p);
                p++;
            }
        }
        ptr++;
    }

    // Initialize merge pointers and allocate result TSVector
    ptr1 = ARRPTR(in1);
    ptr2 = ARRPTR(in2);
    data1 = STRPTR(in1);
    data2 = STRPTR(in2);
    i1 = in1->size;
    i2 = in2->size;

    output_bytes = VARSIZE(in1) + VARSIZE(in2) + i1 + i2;  // Conservative estimate
    out = (TSVector) palloc0(output_bytes);
    SET_VARSIZE(out, output_bytes);
    out->size = in1->size + in2->size;

    ptr = ARRPTR(out);
    data = STRPTR(out);
    dataoff = 0;

    // Three-way merge: both TSVectors have entries
    while (i1 && i2) {
        int cmp = compareEntry(data1, ptr1, data2, ptr2);

        if (cmp < 0) {
            // Copy entry from first TSVector
            ptr->haspos = ptr1->haspos;
            ptr->len = ptr1->len;
            memcpy(data + dataoff, data1 + ptr1->pos, ptr1->len);
            ptr->pos = dataoff;
            dataoff += ptr1->len;

            if (ptr->haspos) {
                dataoff = SHORTALIGN(dataoff);
                memcpy(data + dataoff, _POSVECPTR(in1, ptr1),
                       POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16));
                dataoff += POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16);
            }
            ptr++; ptr1++; i1--;

        } else if (cmp > 0) {
            // Copy entry from second TSVector with position offset
            ptr->haspos = ptr2->haspos;
            ptr->len = ptr2->len;
            memcpy(data + dataoff, data2 + ptr2->pos, ptr2->len);
            ptr->pos = dataoff;
            dataoff += ptr2->len;

            if (ptr->haspos) {
                int addlen = add_pos(in2, ptr2, out, ptr, maxpos);
                if (addlen == 0) {
                    ptr->haspos = 0;
                } else {
                    dataoff = SHORTALIGN(dataoff);
                    dataoff += addlen * sizeof(WordEntryPos) + sizeof(uint16);
                }
            }
            ptr++; ptr2++; i2--;

        } else {
            // Merge entries with same lexeme
            ptr->haspos = ptr1->haspos | ptr2->haspos;
            ptr->len = ptr1->len;
            memcpy(data + dataoff, data1 + ptr1->pos, ptr1->len);
            ptr->pos = dataoff;
            dataoff += ptr1->len;

            if (ptr->haspos) {
                if (ptr1->haspos) {
                    // Copy positions from first TSVector
                    dataoff = SHORTALIGN(dataoff);
                    memcpy(data + dataoff, _POSVECPTR(in1, ptr1),
                           POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16));
                    dataoff += POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16);

                    // Add positions from second TSVector if present
                    if (ptr2->haspos)
                        dataoff += add_pos(in2, ptr2, out, ptr, maxpos) * sizeof(WordEntryPos);
                } else {
                    // Only second TSVector has positions
                    int addlen = add_pos(in2, ptr2, out, ptr, maxpos);
                    if (addlen == 0) {
                        ptr->haspos = 0;
                    } else {
                        dataoff = SHORTALIGN(dataoff);
                        dataoff += addlen * sizeof(WordEntryPos) + sizeof(uint16);
                    }
                }
            }
            ptr++; ptr1++; ptr2++; i1--; i2--;
        }
    }

    // Copy remaining entries from first TSVector
    while (i1) {
        ptr->haspos = ptr1->haspos;
        ptr->len = ptr1->len;
        memcpy(data + dataoff, data1 + ptr1->pos, ptr1->len);
        ptr->pos = dataoff;
        dataoff += ptr1->len;

        if (ptr->haspos) {
            dataoff = SHORTALIGN(dataoff);
            memcpy(data + dataoff, _POSVECPTR(in1, ptr1),
                   POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16));
            dataoff += POSDATALEN(in1, ptr1) * sizeof(WordEntryPos) + sizeof(uint16);
        }
        ptr++; ptr1++; i1--;
    }

    // Copy remaining entries from second TSVector with position offset
    while (i2) {
        ptr->haspos = ptr2->haspos;
        ptr->len = ptr2->len;
        memcpy(data + dataoff, data2 + ptr2->pos, ptr2->len);
        ptr->pos = dataoff;
        dataoff += ptr2->len;

        if (ptr->haspos) {
            int addlen = add_pos(in2, ptr2, out, ptr, maxpos);
            if (addlen == 0) {
                ptr->haspos = 0;
            } else {
                dataoff = SHORTALIGN(dataoff);
                dataoff += addlen * sizeof(WordEntryPos) + sizeof(uint16);
            }
        }
        ptr++; ptr2++; i2--;
    }

    // Check for string length overflow
    if (dataoff > MAXSTRPOS)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("string is too long for tsvector (%d bytes, max %d bytes)",
                             dataoff, MAXSTRPOS)));

    // Finalize output size and compact memory
    int output_size = ptr - ARRPTR(out);
    out->size = output_size;
    if (data != STRPTR(out))
        memmove(STRPTR(out), data, dataoff);
    output_bytes = CALCDATASIZE(out->size, dataoff);
    SET_VARSIZE(out, output_bytes);

    PG_FREE_IF_COPY(in1, 0);
    PG_FREE_IF_COPY(in2, 1);
    PG_RETURN_POINTER(out);
}
```