# varstr_abbrev_convert

## Location
[src/backend/utils/adt/varlena.c:2239-2436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2239-L2436)

## Overview
A sophisticated abbreviation key conversion function that transforms string data into compact Datum representations for optimized sorting performance in PostgreSQL's sort support framework.

## Definition

```c
static Datum
varstr_abbrev_convert(Datum original, SortSupport ssup)
```
## Detailed Description
 is a critical optimization function in PostgreSQL's sort support infrastructure that converts full string values into abbreviated keys (compact representations) that can be compared much more efficiently than full strings. The function employs several strategies based on the collation:

1. **C Locale optimization**: For C locale, directly copies up to 8 bytes from the original string using memcpy(), as memcmp() will be used for comparison
2. **Locale-aware transformation**: For other locales, uses strxfrm() or ICU equivalents to create a transformation blob that preserves collation ordering, then extracts the first 8 bytes
3. **Intelligent caching**: Reuses transformation results when the same string is processed repeatedly
4. **Cardinality tracking**: Uses HyperLogLog to monitor the effectiveness of abbreviation by tracking cardinality of both original and abbreviated keys
5. **Endianness handling**: Converts to big-endian format for consistent cross-platform unsigned integer comparison

The abbreviated keys allow the sort algorithm to perform most comparisons using fast integer operations, falling back to full string comparison only when abbreviated keys are equal.

## Parameters / Member Variables
- `original`: The Datum containing the original string value to be abbreviated
- `ssup`: SortSupport structure containing VarStringSortSupport context with buffers, locale information, and statistics
## Dependencies
- Functions called/Symbols referenced:
  -  - Context structure for string sorting operations
  -  - Extracts VarString from Datum with detoasting
  - ,  - Macros for accessing varlena data and size
  -  - Calculates true length for BPCHAR excluding trailing spaces
  - , ,  - Memory operations for copying and comparison
  - , ,  - Macros for buffer size management
  -  - PostgreSQL memory reallocation function
  -  - Checks if locale supports prefix transformation
  -  - Creates abbreviated transformation for specified prefix length
  -  - Full locale-aware string transformation
  - ,  - Hashing functions for cardinality tracking
  -  - Datum to uint32 conversion
  -  - Adds hash values to HyperLogLog cardinality estimator
  -  - Endianness conversion for cross-platform consistency
  - ,  - Memory management functions
- Called from (representative examples):
  -  - Sets up abbreviation support for string sorting

## Notes and Other Information
- Central to PostgreSQL's string sorting performance optimization, can provide significant speedups
- Handles special cases for bytea (binary data) which may contain NUL bytes
- Uses sophisticated caching strategy to avoid repeated expensive strxfrm() calls
- Monitors abbreviation effectiveness through cardinality estimation using HyperLogLog
- The 8-byte limitation is based on sizeof(Datum) and provides good balance between comparison speed and discrimination
- Endianness conversion ensures that unsigned integer comparison works correctly across different architectures
- Memory management prevents leaks when detoasted copies are created during processing
- Works in conjunction with varstr_abbrev_abort() which can disable abbreviation if it proves ineffective

## Simplified Source

```c
static Datum
varstr_abbrev_convert(Datum original, SortSupport ssup)
{
    const size_t max_prefix_bytes = sizeof(Datum);
    VarStringSortSupport *sss = (VarStringSortSupport *) ssup->ssup_extra;
    VarString *authoritative = DatumGetVarStringPP(original);
    char *authoritative_data = VARDATA_ANY(authoritative);

    // Initialize result buffer
    Datum res;
    char *pres = (char *) &res;
    memset(pres, 0, max_prefix_bytes);

    // Get string length, handling BPCHAR trailing spaces
    int len = VARSIZE_ANY_EXHDR(authoritative);
    if (sss->typid == BPCHAROID)
        len = bpchartruelen(authoritative_data, len);

    // Fast path for C collation - direct memory copy
    if (sss->collate_c) {
        memcpy(pres, authoritative_data, Min(len, max_prefix_bytes));
    }
    else {
        // Locale-aware path using strxfrm transformation

        // Ensure buffer1 is large enough and copy source data
        if (len >= sss->buflen1) {
            sss->buflen1 = Max(len + 1, Min(sss->buflen1 * 2, MaxAllocSize));
            sss->buf1 = repalloc(sss->buf1, sss->buflen1);
        }

        // Check cache for reusable transformation
        if (sss->last_len1 == len && sss->cache_blob &&
            memcmp(sss->buf1, authoritative_data, len) == 0) {
            memcpy(pres, sss->buf2, Min(max_prefix_bytes, sss->last_len2));
            goto done;
        }

        // Copy and null-terminate source string
        memcpy(sss->buf1, authoritative_data, len);
        sss->buf1[len] = '\0';
        sss->last_len1 = len;

        // Transform string using appropriate method
        if (pg_strxfrm_prefix_enabled(sss->locale)) {
            // Use prefix-optimized transformation
            if (sss->buflen2 < max_prefix_bytes) {
                sss->buflen2 = Max(max_prefix_bytes, Min(sss->buflen2 * 2, MaxAllocSize));
                sss->buf2 = repalloc(sss->buf2, sss->buflen2);
            }
            Size bsize = pg_strxfrm_prefix(sss->buf2, sss->buf1, max_prefix_bytes, sss->locale);
            sss->last_len2 = bsize;
        }
        else {
            // Use full transformation with retry loop
            Size bsize;
            for (;;) {
                bsize = pg_strxfrm(sss->buf2, sss->buf1, sss->buflen2, sss->locale);
                sss->last_len2 = bsize;
                if (bsize < sss->buflen2)
                    break;
                // Grow buffer and retry
                sss->buflen2 = Max(bsize + 1, Min(sss->buflen2 * 2, MaxAllocSize));
                sss->buf2 = repalloc(sss->buf2, sss->buflen2);
            }
        }

        // Copy transformation result to abbreviated key
        memcpy(pres, sss->buf2, Min(max_prefix_bytes, bsize));
        sss->cache_blob = true;
    }

    // Track cardinality for abbreviation effectiveness monitoring
    uint32 hash = DatumGetUInt32(hash_any((unsigned char *) authoritative_data,
                                         Min(len, PG_CACHE_LINE_SIZE)));
    if (len > PG_CACHE_LINE_SIZE)
        hash ^= DatumGetUInt32(hash_uint32((uint32) len));
    addHyperLogLog(&sss->full_card, hash);

    // Hash the abbreviated key
#if SIZEOF_DATUM == 8
    uint32 lohalf = (uint32) res;
    uint32 hihalf = (uint32) (res >> 32);
    hash = DatumGetUInt32(hash_uint32(lohalf ^ hihalf));
#else
    hash = DatumGetUInt32(hash_uint32((uint32) res));
#endif
    addHyperLogLog(&sss->abbr_card, hash);

done:
    // Convert to big-endian for consistent cross-platform comparison
    res = DatumBigEndianToNative(res);

    // Clean up temporary detoasted copy if needed
    if (PointerGetDatum(authoritative) != original)
        pfree(authoritative);

    return res;
}
```