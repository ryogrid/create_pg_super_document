# convert_bytea_to_scalar

## Location
[src/backend/utils/adt/selfuncs.c:4739-4786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4739-L4786)

## Overview
Converts bytea (binary data) values to normalized scalar values between 0 and 1 for selectivity estimation, similar to string conversion but handling non-null-terminated binary data with explicit lengths.

## Definition

```c
static void
convert_bytea_to_scalar(Datum value,
						double *scaledvalue,
						Datum lobound,
						double *scaledlobound,
						Datum hibound,
						double *scaledhibound)
```
## Detailed Description
This function performs the core work of  specifically for PostgreSQL's bytea data type, which stores arbitrary binary data. It is conceptually similar to  but handles several key differences:

1. **Non-null-terminated Data**: Unlike strings, bytea data is not null-terminated, so explicit length tracking is required throughout the conversion process.

2. **Full Byte Range**: The function assumes a uniform distribution across all possible byte values (0-255), without the character class optimizations used for strings, since binary data doesn't follow text patterns.

3. **Length-based Processing**: Uses VARSIZE_ANY_EXHDR() to determine actual data lengths and VARDATA_ANY() to access the raw byte data.

4. **Common Prefix Stripping**: Like the string version, it strips common prefixes from all three inputs to focus on the distinguishing portions of the data.

The conversion process mirrors the string algorithm but operates on raw byte arrays with explicit length management, making it suitable for any binary data including images, encrypted data, or serialized objects.

## Parameters / Member Variables
- : The bytea Datum to be converted to a scalar
- : Output pointer for the scaled value of the input bytea
- : Lower bound bytea Datum from histogram data
- : Output pointer for the scaled value of the lower bound
- : Upper bound bytea Datum from histogram data
- : Output pointer for the scaled value of the upper bound

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetByteaPP (extracts bytea pointer from Datum, called 3 times)
  - [convert_one_bytea_to_scalar](convert_one_bytea_to_scalar.md) (performs actual conversion, called 3 times)
  - VARSIZE_ANY_EXHDR (macro to get data size excluding header)
  - VARDATA_ANY (macro to access raw data)
  - Min (utility macro for minimum value)
- Called from (representative examples):
  - [convert_to_scalar](convert_to_scalar.md)

## Notes and Other Information
- The function is static, indicating it's an internal implementation detail of the selfuncs.c module
- Uses a fixed range of 0-255 for all bytes, unlike the dynamic range analysis in string conversion
- The comment suggests future enhancement might involve storing actual byte range statistics in pg_statistic
- Common prefix stripping is particularly valuable for binary data with headers or common structural patterns
- Part of PostgreSQL's query planner's selectivity estimation system for binary data comparisons
- The uniform distribution assumption may be less accurate for structured binary data but provides a reasonable baseline for estimation
- Memory management relies on the bytea structure's built-in length information rather than null-termination

## Simplified Source

```c
static void
convert_bytea_to_scalar(Datum value, double *scaledvalue,
                       Datum lobound, double *scaledlobound,
                       Datum hibound, double *scaledhibound)
{
    // Extract bytea values and get their lengths
    bytea *valuep = DatumGetByteaPP(value);
    bytea *loboundp = DatumGetByteaPP(lobound);
    bytea *hiboundp = DatumGetByteaPP(hibound);

    int valuelen = VARSIZE_ANY_EXHDR(valuep);
    int loboundlen = VARSIZE_ANY_EXHDR(loboundp);
    int hiboundlen = VARSIZE_ANY_EXHDR(hiboundp);

    unsigned char *valstr = (unsigned char *) VARDATA_ANY(valuep);
    unsigned char *lostr = (unsigned char *) VARDATA_ANY(loboundp);
    unsigned char *histr = (unsigned char *) VARDATA_ANY(hiboundp);

    // Use full byte range (0-255) for bytea data
    int rangelo = 0, rangehi = 255;

    // Strip common prefix from all three strings
    int minlen = Min(Min(valuelen, loboundlen), hiboundlen);
    for (int i = 0; i < minlen; i++) {
        if (*lostr != *histr || *lostr != *valstr)
            break;
        lostr++; histr++; valstr++;
        loboundlen--; hiboundlen--; valuelen--;
    }

    // Convert to scalar values
    *scaledvalue = convert_one_bytea_to_scalar(valstr, valuelen, rangelo, rangehi);
    *scaledlobound = convert_one_bytea_to_scalar(lostr, loboundlen, rangelo, rangehi);
    *scaledhibound = convert_one_bytea_to_scalar(histr, hiboundlen, rangelo, rangehi);
}
```