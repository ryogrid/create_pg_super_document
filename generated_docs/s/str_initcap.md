# str_initcap

## Location
[src/backend/utils/adt/formatting.c:1973-2157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1973-L2157)

## Overview
A collation-aware, wide-character-aware function that converts the first letter of each word to uppercase and the rest to lowercase, supporting multiple collation providers including ICU, built-in Unicode, and libc.

## Definition
```c
char *str_initcap(const char *buff, size_t nbytes, Oid collid)
```

## Detailed Description
The `str_initcap` function implements initial capitalization (title case) functionality that respects database collation settings. It capitalizes the first letter of each word while converting all other letters to lowercase. The function handles multiple encoding scenarios and collation providers:

1. **C/POSIX Collations**: Uses ASCII-only conversion via `asc_initcap`
2. **ICU Provider**: Leverages ICU library functions (`u_strToTitle_default_BI`) for Unicode-aware title case conversion
3. **Built-in Provider**: Uses PostgreSQL's internal Unicode conversion (`unicode_strtitle`) with custom word boundary detection via `initcap_wbnext`
4. **libc Provider**: Implements character-by-character processing using wide character functions for multibyte encodings, or byte-by-byte for single-byte encodings

The function uses a `wasalnum` flag to track whether the previous character was alphanumeric, allowing it to determine when to capitalize (first character of a word) versus when to make lowercase (subsequent characters in a word).

## Parameters / Member Variables
- `buff`: Input string buffer to convert (can be null)
- `nbytes`: Number of bytes in the input buffer
- `collid`: OID of the collation to use for case conversion and word boundary detection

## Dependencies
- Functions called/Symbols referenced:
  - [lc_ctype_is_c](../l/lc_ctype_is_c.md): Check if collation uses C/POSIX locale
  - [asc_initcap](../a/asc_initcap.md): ASCII-only initial capitalization
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md): Get locale information from collation OID
  - [icu_to_uchar](../i/icu_to_uchar.md), `icu_convert_case`, `icu_from_uchar`: ICU conversion functions
  - [unicode_strtitle](../u/unicode_strtitle.md): Built-in Unicode title case conversion
  - [initcap_wbnext](../i/initcap_wbnext.md): Custom word boundary iterator for built-in provider
  - [WordBoundaryState](../W/WordBoundaryState.md): State structure for word boundary detection
  - [char2wchar](../c/char2wchar.md), `wchar2char`: Wide character conversion functions
  - `towlower_l`, `towupper_l`, `iswalnum_l`: Locale-aware wide character functions
  - `tolower_l`, `toupper_l`, `isalnum_l`: Locale-aware character functions
  - [pg_tolower](../p/pg_tolower.md), `pg_toupper`: PostgreSQL's ASCII case conversion
- Called from (representative examples):
  - [initcap](../i/initcap.md): SQL INITCAP() function implementation
  - [str_initcap_z](str_initcap_z.md): Null-terminated string wrapper

## Notes and Other Information
- Returns a palloc'd, null-terminated string that must be freed by the caller
- Throws an error if collation OID is invalid or indeterminate
- For multibyte encodings with libc provider, uses wide character functions to ensure proper handling
- The function assumes database character encoding matches LC_CTYPE encoding
- Memory allocation is carefully managed with overflow protection for large strings
- Built-in provider uses a custom word boundary iterator (`initcap_wbnext`) that defines word boundaries as transitions between alphanumeric and non-alphanumeric characters
- The algorithm maintains state between characters to track word boundaries and apply appropriate case conversion
- ICU provider uses the most sophisticated word boundary detection following Unicode standards

## Simplified Source

```c
char *str_initcap(const char *buff, size_t nbytes, Oid collid) {
    char *result;
    int wasalnum = false;

    if (!buff) return NULL;

    // Validate collation
    if (!OidIsValid(collid)) {
        ereport(ERROR, "could not determine collation for initcap()");
    }

    // C/POSIX: Use ASCII-only conversion
    if (lc_ctype_is_c(collid)) {
        result = asc_initcap(buff, nbytes);
    }
    else {
        pg_locale_t mylocale = pg_newlocale_from_collation(collid);

        // ICU provider: Use Unicode-aware title case
        if (mylocale && mylocale->provider == COLLPROVIDER_ICU) {
            // Convert to Unicode, apply title case, convert back
            UChar *buff_uchar, *buff_conv;
            int32_t len_uchar = icu_to_uchar(&buff_uchar, buff, nbytes);
            int32_t len_conv = icu_convert_case(u_strToTitle_default_BI,
                                              mylocale, &buff_conv,
                                              buff_uchar, len_uchar);
            icu_from_uchar(&result, buff_conv, len_conv);
            pfree(buff_uchar);
            pfree(buff_conv);
        }
        // Built-in provider: Use internal Unicode conversion
        else if (mylocale && mylocale->provider == COLLPROVIDER_BUILTIN) {
            struct WordBoundaryState wbstate = {
                .str = buff, .len = nbytes, .offset = 0,
                .init = false, .prev_alnum = false
            };

            size_t dstsize = nbytes + 1;
            char *dst = palloc(dstsize);
            size_t needed = unicode_strtitle(dst, dstsize, buff, nbytes,
                                           initcap_wbnext, &wbstate);

            // Resize buffer if needed
            if (needed + 1 > dstsize) {
                wbstate.offset = 0;
                wbstate.init = false;
                dstsize = needed + 1;
                dst = repalloc(dst, dstsize);
                unicode_strtitle(dst, dstsize, buff, nbytes,
                               initcap_wbnext, &wbstate);
            }
            result = dst;
        }
        // libc provider: Character-by-character processing
        else {
            if (pg_database_encoding_max_length() > 1) {
                // Multibyte encoding: use wide character functions
                wchar_t *workspace = palloc((nbytes + 1) * sizeof(wchar_t));
                char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

                // Process each character
                for (size_t i = 0; workspace[i] != 0; i++) {
                    if (wasalnum) {
                        workspace[i] = towlower_l(workspace[i], mylocale->info.lt);
                    } else {
                        workspace[i] = towupper_l(workspace[i], mylocale->info.lt);
                    }
                    wasalnum = iswalnum_l(workspace[i], mylocale->info.lt);
                }

                // Convert back to multibyte string
                size_t result_size = i * pg_database_encoding_max_length() + 1;
                result = palloc(result_size);
                wchar2char(result, workspace, result_size, mylocale);
                pfree(workspace);
            }
            else {
                // Single-byte encoding: direct character processing
                result = pnstrdup(buff, nbytes);
                for (char *p = result; *p; p++) {
                    if (wasalnum) {
                        *p = tolower_l((unsigned char) *p, mylocale->info.lt);
                    } else {
                        *p = toupper_l((unsigned char) *p, mylocale->info.lt);
                    }
                    wasalnum = isalnum_l((unsigned char) *p, mylocale->info.lt);
                }
            }
        }
    }

    return result;
}
```