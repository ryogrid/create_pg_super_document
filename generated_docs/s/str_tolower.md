# str_tolower

## Location
[src/backend/utils/adt/formatting.c:1636-1783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1636-L1783)

## Overview
A collation-aware, wide-character-aware function that converts a string to lowercase, supporting multiple collation providers including ICU, built-in Unicode, and libc.

## Definition
```c
char *str_tolower(const char *buff, size_t nbytes, Oid collid)
```

## Detailed Description
The `str_tolower` function provides robust lowercase conversion functionality that respects database collation settings. It handles multiple encoding scenarios and collation providers:

1. **C/POSIX Collations**: Uses ASCII-only conversion via `asc_tolower`
2. **ICU Provider**: Leverages ICU library functions for Unicode-aware case conversion
3. **Built-in Provider**: Uses PostgreSQL's internal Unicode conversion for UTF-8 databases
4. **libc Provider**: Falls back to system locale functions, with special handling for multibyte encodings

The function automatically detects the appropriate conversion method based on the collation and database encoding, ensuring correct case conversion across different locales and character sets.

## Parameters / Member Variables
- `buff`: Input string buffer to convert (can be null)
- `nbytes`: Number of bytes in the input buffer
- `collid`: OID of the collation to use for case conversion

## Dependencies
- Functions called/Symbols referenced:
  - [lc_ctype_is_c](../l/lc_ctype_is_c.md): Check if collation uses C/POSIX locale
  - [asc_tolower](../a/asc_tolower.md): ASCII-only lowercase conversion
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md): Get locale information from collation OID
  - [icu_to_uchar](../i/icu_to_uchar.md), `icu_convert_case`, `icu_from_uchar`: ICU conversion functions
  - [unicode_strlower](../u/unicode_strlower.md): Built-in Unicode lowercase conversion
  - [char2wchar](../c/char2wchar.md), `wchar2char`: Wide character conversion functions
  - `towlower_l`, `tolower_l`: Locale-aware case conversion
  - [pg_tolower](../p/pg_tolower.md): PostgreSQL's ASCII case conversion
- Called from (representative examples):
  - [lower](../l/lower.md): SQL LOWER() function implementation
  - [seq_search_localized](seq_search_localized.md): Localized pattern searching
  - [str_tolower_z](str_tolower_z.md): Null-terminated string wrapper

## Notes and Other Information
- Returns a palloc'd, null-terminated string that must be freed by the caller
- Throws an error if collation OID is invalid or indeterminate
- For multibyte encodings with libc provider, uses wide character functions to ensure proper handling
- The function assumes database character encoding matches LC_CTYPE encoding
- Memory allocation is carefully managed with overflow protection for large strings
- Special handling ensures ASCII I/i behavior in default collations while respecting locale-specific rules in non-default collations

## Simplified Source

```c
char *
str_tolower(const char *buff, size_t nbytes, Oid collid)
{
    char *result;

    if (!buff)
        return NULL;

    // Validate collation OID
    if (!OidIsValid(collid))
        ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_COLLATION),
                       errmsg("could not determine which collation to use for lower() function")));

    // Handle C/POSIX collations with ASCII conversion
    if (lc_ctype_is_c(collid))
    {
        result = asc_tolower(buff, nbytes);
    }
    else
    {
        pg_locale_t mylocale = pg_newlocale_from_collation(collid);

#ifdef USE_ICU
        // ICU provider: use ICU case conversion
        if (mylocale && mylocale->provider == COLLPROVIDER_ICU)
        {
            UChar *buff_uchar, *buff_conv;
            int32_t len_uchar = icu_to_uchar(&buff_uchar, buff, nbytes);
            int32_t len_conv = icu_convert_case(u_strToLower, mylocale,
                                              &buff_conv, buff_uchar, len_uchar);
            icu_from_uchar(&result, buff_conv, len_conv);
            pfree(buff_uchar);
            pfree(buff_conv);
        }
        else
#endif
        // Built-in provider: use Unicode conversion for UTF-8
        if (mylocale && mylocale->provider == COLLPROVIDER_BUILTIN)
        {
            size_t dstsize = nbytes + 1;
            char *dst = palloc(dstsize);
            size_t needed = unicode_strlower(dst, dstsize, buff, nbytes);

            // Resize buffer if needed
            if (needed + 1 > dstsize)
            {
                dstsize = needed + 1;
                dst = repalloc(dst, dstsize);
                unicode_strlower(dst, dstsize, buff, nbytes);
            }
            result = dst;
        }
        else
        {
            // libc provider: handle multibyte vs single-byte encodings
            if (pg_database_encoding_max_length() > 1)
            {
                // Multibyte encoding: use wide character functions
                wchar_t *workspace = palloc((nbytes + 1) * sizeof(wchar_t));
                char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

                // Convert each character to lowercase
                for (size_t i = 0; workspace[i] != 0; i++)
                {
                    workspace[i] = mylocale ?
                        towlower_l(workspace[i], mylocale->info.lt) :
                        towlower(workspace[i]);
                }

                // Convert back to multibyte
                size_t result_size = nbytes * pg_database_encoding_max_length() + 1;
                result = palloc(result_size);
                wchar2char(result, workspace, result_size, mylocale);
                pfree(workspace);
            }
            else
            {
                // Single-byte encoding: convert in place
                result = pnstrdup(buff, nbytes);
                for (char *p = result; *p; p++)
                {
                    *p = mylocale ?
                        tolower_l((unsigned char) *p, mylocale->info.lt) :
                        pg_tolower((unsigned char) *p);
                }
            }
        }
    }

    return result;
}
```