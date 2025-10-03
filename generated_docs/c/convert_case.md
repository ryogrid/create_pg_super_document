# convert_case

## Location
[src/common/unicode_case.c:137-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_case.c#L137-L202)

## Overview
Core function that performs Unicode case conversion (lowercase, uppercase, or titlecase) on UTF-8 encoded strings with support for word boundary detection.

## Definition

```c
static size_t
convert_case(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			 CaseKind str_casekind, WordBoundaryNext wbnext, void *wbstate)
```
## Detailed Description
The  function is the central implementation for all case conversion operations in PostgreSQL's Unicode handling. It supports three types of case conversion: lowercase (), uppercase (), and titlecase (). 

For lowercase and uppercase conversions, it maps each character in the string using the Unicode case mapping table. For titlecase conversion, it uses word boundary detection to map characters at word boundaries to uppercase and other characters to lowercase.

The function processes the source string character by character, converting each Unicode codepoint according to the specified case kind. It uses the  function to locate case mapping information for each character and handles both simple character mapping and characters without case mappings by copying them unchanged.

## Parameters / Member Variables
- `*dst`: Destination buffer to store the converted result
- `dstsize`: Size of the destination buffer in bytes
- `*src`: Source UTF-8 encoded string to convert
- `srclen`: Length of source string in bytes, or negative for NUL-terminated strings
- `str_casekind`: Type of case conversion (CaseLower, CaseUpper, or CaseTitle)
- `wbnext`: Word boundary detection function (required for titlecase, NULL otherwise)
- `*wbstate`: State for word boundary detection (required for titlecase, NULL otherwise)
## Dependencies
- Functions called/Symbols referenced:
  - [utf8_to_unicode](../u/utf8_to_unicode.md) (converts UTF-8 bytes to Unicode codepoint)
  - [unicode_utf8len](../u/unicode_utf8len.md) (calculates UTF-8 byte length for a Unicode codepoint)
  - [find_case_map](../f/find_case_map.md) (finds case mapping for a Unicode codepoint)
  - [unicode_to_utf8](../u/unicode_to_utf8.md) (converts Unicode codepoint back to UTF-8 bytes)
  - pg_case_map (Unicode case mapping structure)
  - [CaseKind](../C/CaseKind.md) (enumeration for case conversion types)
- Called from (representative examples):
  - [unicode_strlower](../u/unicode_strlower.md)
  - [unicode_strtitle](../u/unicode_strtitle.md)  
  - [unicode_strupper](../u/unicode_strupper.md)

## Notes and Other Information
- This is a static function, only accessible within the unicode_case.c file
- Handles proper Unicode case conversion according to Unicode standards
- For titlecase conversion, requires word boundary detection functions to determine where to apply uppercase vs lowercase
- Returns the total length of the converted result, even if the destination buffer was insufficient
- Performs assertions to ensure titlecase parameters are properly provided when needed
- The function gracefully handles buffer overflow by continuing to calculate the correct result length while only writing what fits in the destination buffer

## Simplified Source

```c
static size_t
convert_case(char *dst, size_t dstsize, const char *src, ssize_t srclen,
             CaseKind str_casekind, WordBoundaryNext wbnext, void *wbstate)
{
    CaseKind chr_casekind = str_casekind;
    size_t srcoff = 0;
    size_t result_len = 0;
    size_t boundary = 0;

    // Initialize word boundary for titlecase
    if (str_casekind == CaseTitle) {
        boundary = wbnext(wbstate);
    }

    // Process each character in the source string
    while ((srclen < 0 || srcoff < srclen) && src[srcoff] != '\0') {
        pg_wchar u1 = utf8_to_unicode((unsigned char *) src + srcoff);
        int u1len = unicode_utf8len(u1);
        const pg_case_map *casemap = find_case_map(u1);

        // Update case kind for titlecase at word boundaries
        if (str_casekind == CaseTitle) {
            if (srcoff == boundary) {
                chr_casekind = CaseUpper;
                boundary = wbnext(wbstate);
            } else {
                chr_casekind = CaseLower;
            }
        }

        // Perform case conversion
        if (casemap) {
            pg_wchar u2 = casemap->simplemap[chr_casekind];
            pg_wchar u2len = unicode_utf8len(u2);

            // Write converted character if space available
            if (result_len + u2len <= dstsize)
                unicode_to_utf8(u2, (unsigned char *) dst + result_len);

            result_len += u2len;
        } else {
            // No mapping available, copy original character
            if (result_len + u1len <= dstsize)
                memcpy(dst + result_len, src + srcoff, u1len);

            result_len += u1len;
        }

        srcoff += u1len;
    }

    // Null-terminate if space available
    if (result_len < dstsize)
        dst[result_len] = '\0';

    return result_len;
}
```