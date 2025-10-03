# pg_encoding_mbcliplen

## Location
[src/backend/utils/mb/mbutils.c:1093-1124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1093-L1124)

## Overview
Clips a multi-byte string to a specified length limit with a given encoding, ensuring the result remains valid by not breaking multi-byte character boundaries.

## Definition

```c
int
pg_encoding_mbcliplen(int encoding, const char *mbstr,
					  int len, int limit)
```
## Detailed Description
This function calculates the maximum number of bytes that can be taken from a multi-byte string without exceeding the specified limit and without breaking multi-byte character boundaries. It handles different character encodings by using the appropriate mblen function for each encoding to determine character byte lengths. For single-byte encodings, it optimizes by calling the simpler cliplen function directly.

The function iterates through the string character by character, accumulating the byte length until adding the next character would exceed the limit. It ensures that the clipped string remains valid in the specified encoding by respecting multi-byte character boundaries.

## Parameters / Member Variables
- `encoding`: The character encoding identifier (e.g., UTF-8, EUC_JP, etc.)
- `*mbstr`: Pointer to the input multi-byte string to be clipped
- `len`: The length of the input string in bytes
- `limit`: The maximum number of bytes allowed in the result
## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_max_length](pg_encoding_max_length.md)
  - [cliplen](../c/cliplen.md)
  - pg_wchar_table (array access for mblen function pointer)
- Called from (representative examples):
  - [pg_mbcliplen](pg_mbcliplen.md)

## Notes and Other Information
- The function assumes the input string is valid in the specified encoding
- For single-byte encodings, it delegates to the more efficient cliplen function
- The function stops when either the limit is reached or the end of string is encountered
- Returns the actual number of bytes that can be safely taken without breaking character boundaries

## Simplified Source

```c
int pg_encoding_mbcliplen(int encoding, const char *mbstr, int len, int limit)
{
    mblen_converter mblen_fn;
    int clen = 0;
    int l;

    // Optimization for single-byte encodings
    if (pg_encoding_max_length(encoding) == 1)
        return cliplen(mbstr, len, limit);

    // Get the length function for this encoding
    mblen_fn = pg_wchar_table[encoding].mblen;

    // Process each character, respecting multi-byte boundaries
    while (len > 0 && *mbstr)
    {
        // Get length of current character
        l = (*mblen_fn)((const unsigned char *) mbstr);

        // Stop if adding this character would exceed limit
        if ((clen + l) > limit)
            break;

        // Add character length to cumulative length
        clen += l;

        // Stop if we've reached exact limit
        if (clen == limit)
            break;

        // Move to next character
        len -= l;
        mbstr += l;
    }

    return clen;
}
```