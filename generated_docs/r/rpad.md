# rpad

## Location
[src/backend/utils/adt/oracle_compat.c:245-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L245-L341)

## Overview
The  function right-pads a string to a specified length with a padding string, or truncates the string if it's longer than the specified length.

## Definition

```c
Datum
rpad(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function takes three parameters: a source string, a target length, and a padding string. It returns the source string right-padded to the specified length using the padding string. If the source string is longer than the target length, it truncates the string on the right to the target length. The padding string is repeated as necessary to fill the required space on the right side of the source string. The function handles multibyte characters correctly and includes overflow protection for very large requested lengths.

## Parameters / Member Variables
- : PostgreSQL function argument structure containing:
  - Input parameter 0:  - The source string to be padded or truncated
  - Input parameter 1:  - The target length for the result string
  - Input parameter 2:  - The padding string to use for right-padding

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text arguments from function arguments
  -  - Extract integer argument
  -  - Get size of variable-length data excluding header
  -  - Get pointer to variable-length data
  -  - Calculate multibyte string length
  -  - Get maximum bytes per character for database encoding
  -  - Safe 32-bit multiplication with overflow check
  -  - Safe 32-bit addition with overflow check
  -  - Validate memory allocation size
  -  - Report errors
  -  - Allocate memory
  -  - Get pointer to variable-length data
  -  - Get length of multibyte character
  -  - Copy memory
  -  - Set size of variable-length data
  -  - Return text result to PostgreSQL

- Called from (representative examples):
  - SQL queries using the  function
  - PostgreSQL query executor

## Notes and Other Information
- Located in  at lines 245-341
- Part of PostgreSQL's Oracle compatibility functions
- Complementary function to  for string padding operations
- Handles multibyte characters correctly by using  and related functions
- Includes overflow protection to prevent memory allocation issues with extremely large lengths
- Negative target lengths are silently treated as zero
- If the padding string is empty, no padding is performed (result length equals source string length or target length, whichever is smaller)
- The padding string is repeated cyclically if needed to fill the required space
- Unlike , this function first copies the source string then appends the padding
- Memory is properly allocated and the result is returned as a PostgreSQL text type

## Simplified Source

```c
Datum
rpad(PG_FUNCTION_ARGS)
{
    // Get function arguments
    text *string1 = PG_GETARG_TEXT_PP(0);  // source string
    int32 len = PG_GETARG_INT32(1);        // target length
    text *string2 = PG_GETARG_TEXT_PP(2);  // padding string

    // Handle negative length
    if (len < 0) len = 0;

    // Get string lengths and data pointers
    int s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1), VARSIZE_ANY_EXHDR(string1));
    int s2len = VARSIZE_ANY_EXHDR(string2);

    // Truncate source if longer than target
    if (s1len > len) s1len = len;

    // Skip padding if padding string is empty
    if (s2len <= 0) len = s1len;

    // Allocate result buffer with overflow protection
    int bytelen;
    if (unlikely(pg_mul_s32_overflow(pg_database_encoding_max_length(), len, &bytelen)) ||
        unlikely(pg_add_s32_overflow(bytelen, VARHDRSZ, &bytelen)) ||
        unlikely(!AllocSizeIsValid(bytelen)))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("requested length too large")));

    text *ret = (text *) palloc(bytelen);
    char *ptr_ret = VARDATA(ret);

    // Copy source string first
    char *str_ptr = VARDATA_ANY(string1);
    while (s1len-- > 0) {
        int mlen = pg_mblen(str_ptr);
        memcpy(ptr_ret, str_ptr, mlen);
        ptr_ret += mlen;
        str_ptr += mlen;
    }

    // Add padding characters after source (right padding)
    int padding_needed = len - pg_mbstrlen_with_len(VARDATA_ANY(string1), VARSIZE_ANY_EXHDR(string1));
    char *pad_ptr = VARDATA_ANY(string2);
    char *pad_end = pad_ptr + s2len;

    while (padding_needed-- > 0) {
        int mlen = pg_mblen(pad_ptr);
        memcpy(ptr_ret, pad_ptr, mlen);
        ptr_ret += mlen;
        pad_ptr += mlen;
        if (pad_ptr == pad_end) pad_ptr = VARDATA_ANY(string2);  // wrap around
    }

    SET_VARSIZE(ret, ptr_ret - (char *) ret);
    PG_RETURN_TEXT_P(ret);
}
```