# tsvector_filter

## Location
[src/backend/utils/adt/tsvector_op.c:819-924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L819-L924)

## Overview
Filters a tsvector to keep only lexemes with specified weights, returning a new tsvector containing only those lexemes that have positions with the given weight values.

## Definition

```c
struct_array_builtin(weights, CHAROID, &dweights, &nulls, &nweights);
```
## Detailed Description
The  function implements the  PostgreSQL function which processes a tsvector and an array of weight characters (A, B, C, D), keeping only those lexemes that have positional information matching the specified weights. The function creates a filtered copy of the input tsvector by examining each lexeme's positional data and retaining only positions whose weights match those in the provided weight array.

The function builds a bitmask from the input weight array where each weight character ('A'/'a', 'B'/'b', 'C'/'c', 'D'/'d') corresponds to a specific bit position. It then iterates through each lexeme in the input tsvector, checking if any of its positions have weights matching the mask. Lexemes without matching positions are excluded from the output.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function argument structure containing:
  - Argument 0: Input tsvector to be filtered
  - Argument 1: Array of weight characters (A, B, C, D) to filter by

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extract tsvector from function arguments
  - PG_GETARG_ARRAYTYPE_P: Extract array from function arguments  
  - ARRPTR: Get pointer to WordEntry array in tsvector
  - STRPTR: Get pointer to string data in tsvector
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md): Decompose input weight array
  - [DatumGetChar](../D/DatumGetChar.md): Convert Datum to char
  - _POSVECPTR: Get pointer to position vector for a word entry
  - WEP_GETWEIGHT: Extract weight from a word entry position
  - SHORTALIGN: Align memory addresses
  - POSDATALEN: Calculate position data length
  - CALCDATASIZE: Calculate total data size for tsvector
  - SET_VARSIZE: Set variable-length type size
  - PG_RETURN_POINTER: Return result pointer

- Called from (representative examples):
  - No direct callers found (exposed as PostgreSQL SQL function)

## Notes and Other Information
- The function only processes lexemes that have positional information ( is true)
- Weight characters are case-insensitive (both uppercase and lowercase accepted)
- Invalid weight characters cause an error with ERRCODE_INVALID_PARAMETER_VALUE
- NULL values in the weight array are not allowed and trigger ERRCODE_NULL_VALUE_NOT_ALLOWED
- The output tsvector is allocated with the same initial size as input but may be smaller after filtering
- Memory is realigned and compacted in the final result to minimize storage space
- Uses bitmask operations for efficient weight matching (A=8, B=4, C=2, D=1)

## Simplified Source

```c
Datum tsvector_filter(PG_FUNCTION_ARGS) {
    TSVector tsin = PG_GETARG_TSVECTOR(0);
    ArrayType *weights = PG_GETARG_ARRAYTYPE_P(1);
    WordEntry *arrin = ARRPTR(tsin);
    WordEntry *arrout;
    char *datain = STRPTR(tsin);
    char *dataout;
    Datum *dweights;
    bool *nulls;
    int nweights;
    char mask = 0;

    // Extract weight array
    deconstruct_array_builtin(weights, CHAROID, &dweights, &nulls, &nweights);

    // Build weight mask from input characters
    for (int i = 0; i < nweights; i++) {
        if (nulls[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                           errmsg("weight array may not contain nulls")));

        char char_weight = DatumGetChar(dweights[i]);
        switch (char_weight) {
            case 'A': case 'a': mask |= 8; break;  // Weight A
            case 'B': case 'b': mask |= 4; break;  // Weight B
            case 'C': case 'c': mask |= 2; break;  // Weight C
            case 'D': case 'd': mask |= 1; break;  // Weight D
            default:
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("unrecognized weight: \"%c\"", char_weight)));
        }
    }

    // Allocate output tsvector
    TSVector tsout = (TSVector) palloc0(VARSIZE(tsin));
    tsout->size = tsin->size;
    arrout = ARRPTR(tsout);
    dataout = STRPTR(tsout);

    // Filter lexemes based on weight mask
    int cur_pos = 0;
    int j = 0;
    for (int i = 0; i < tsin->size; i++) {
        if (!arrin[i].haspos) continue;  // Skip lexemes without positions

        WordEntryPosVector *posvin = _POSVECPTR(tsin, arrin + i);
        WordEntryPosVector *posvout = (WordEntryPosVector *)
            (dataout + SHORTALIGN(cur_pos + arrin[i].len));

        // Copy positions that match weight mask
        int npos = 0;
        for (int k = 0; k < posvin->npos; k++) {
            if (mask & (1 << WEP_GETWEIGHT(posvin->pos[k])))
                posvout->pos[npos++] = posvin->pos[k];
        }

        // Skip lexeme if no matching positions found
        if (!npos) continue;

        // Copy lexeme data to output
        arrout[j].haspos = true;
        arrout[j].len = arrin[i].len;
        arrout[j].pos = cur_pos;
        memcpy(dataout + cur_pos, datain + arrin[i].pos, arrin[i].len);
        posvout->npos = npos;

        // Update position for next lexeme
        cur_pos += SHORTALIGN(arrin[i].len);
        cur_pos += POSDATALEN(tsout, arrout + j) * sizeof(WordEntryPos) + sizeof(uint16);
        j++;
    }

    // Finalize output tsvector
    tsout->size = j;
    if (dataout != STRPTR(tsout))
        memmove(STRPTR(tsout), dataout, cur_pos);
    SET_VARSIZE(tsout, CALCDATASIZE(tsout->size, cur_pos));

    PG_FREE_IF_COPY(tsin, 0);
    PG_RETURN_POINTER(tsout);
}
```