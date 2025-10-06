# dotrim

## Location
[src/backend/utils/adt/oracle_compat.c:378-533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L378-L533)

## Overview
The dotrim function is the core implementation that provides string trimming functionality for PostgreSQL's trim functions, supporting both single-byte and multibyte character encodings.

## Definition
```c
static text *dotrim(const char *string, int stringlen,
                   const char *set, int setlen,
                   bool doltrim, bool dortrim)
```

## Detailed Description
dotrim is a static helper function that implements the common trimming logic used by btrim, ltrim, rtrim and their single-parameter variants. It can remove characters from the left side, right side, or both sides of a string based on a specified character set. The function is optimized to handle both single-byte and multibyte character encodings efficiently, using different algorithms for each case. For multibyte encodings, it builds arrays of character pointers to avoid inefficient character boundary checks in inner loops.

## Parameters / Member Variables
- `string`: Pointer to the input string data to be trimmed
- `stringlen`: Length of the input string in bytes
- `set`: Pointer to the character set data defining which characters to remove
- `setlen`: Length of the character set in bytes
- `doltrim`: Boolean flag to enable trimming from the left (start) of the string
- `dortrim`: Boolean flag to enable trimming from the right (end) of the string

## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (check for multibyte encoding)
  - [pg_mblen](../p/pg_mblen.md) (get multibyte character length)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (convert result to text type)
- Called from (representative examples):
  - [btrim](../b/btrim.md) (bidirectional trimming)
  - [btrim1](../b/btrim1.md) (bidirectional whitespace trimming)
  - [ltrim](../l/ltrim.md) (left-side trimming)
  - [ltrim1](../l/ltrim1.md) (left-side whitespace trimming)
  - [rtrim](../r/rtrim.md) (right-side trimming)
  - [rtrim1](../r/rtrim1.md) (right-side whitespace trimming)

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:378-533
- Uses two different algorithms: optimized single-byte handling and more complex multibyte character handling
- For multibyte encodings, builds temporary arrays to map character positions and lengths
- Memory management includes proper cleanup of allocated arrays with pfree()
- Returns early if either input string or character set is empty
- The function is static, meaning it's only accessible within the same source file

## Simplified Source

```c
static text *
dotrim(const char *string, int stringlen,
       const char *set, int setlen,
       bool doltrim, bool dortrim)
{
    // Return early if either string or set is empty
    if (stringlen <= 0 || setlen <= 0)
        return cstring_to_text_with_len(string, stringlen);

    if (pg_database_encoding_max_length() > 1) {
        // Multibyte encoding path: build character arrays for efficiency
        const char **stringchars = (const char **) palloc(stringlen * sizeof(char *));
        const char **setchars = (const char **) palloc(setlen * sizeof(char *));
        int *stringmblen = (int *) palloc(stringlen * sizeof(int));
        int *setmblen = (int *) palloc(setlen * sizeof(int));

        // Build string character array
        int stringnchars = 0;
        const char *p = string;
        int len = stringlen;
        while (len > 0) {
            stringchars[stringnchars] = p;
            int mblen = pg_mblen(p);
            stringmblen[stringnchars] = mblen;
            stringnchars++;
            p += mblen;
            len -= mblen;
        }

        // Build set character array
        int setnchars = 0;
        p = set;
        len = setlen;
        while (len > 0) {
            setchars[setnchars] = p;
            int mblen = pg_mblen(p);
            setmblen[setnchars] = mblen;
            setnchars++;
            p += mblen;
            len -= mblen;
        }

        int resultndx = 0;
        int resultnchars = stringnchars;

        // Left trim: remove matching characters from start
        if (doltrim) {
            while (resultnchars > 0) {
                const char *str_pos = stringchars[resultndx];
                int str_len = stringmblen[resultndx];
                bool found = false;

                for (int i = 0; i < setnchars; i++) {
                    if (str_len == setmblen[i] &&
                        memcmp(str_pos, setchars[i], str_len) == 0) {
                        found = true;
                        break;
                    }
                }
                if (!found) break;

                string += str_len;
                stringlen -= str_len;
                resultndx++;
                resultnchars--;
            }
        }

        // Right trim: remove matching characters from end
        if (dortrim) {
            while (resultnchars > 0) {
                const char *str_pos = stringchars[resultndx + resultnchars - 1];
                int str_len = stringmblen[resultndx + resultnchars - 1];
                bool found = false;

                for (int i = 0; i < setnchars; i++) {
                    if (str_len == setmblen[i] &&
                        memcmp(str_pos, setchars[i], str_len) == 0) {
                        found = true;
                        break;
                    }
                }
                if (!found) break;

                stringlen -= str_len;
                resultnchars--;
            }
        }

        // Clean up allocated arrays
        pfree(stringchars);
        pfree(stringmblen);
        pfree(setchars);
        pfree(setmblen);
    } else {
        // Single-byte encoding path: simple byte comparison
        if (doltrim) {
            while (stringlen > 0) {
                char str_ch = *string;
                bool found = false;
                for (int i = 0; i < setlen; i++) {
                    if (str_ch == set[i]) {
                        found = true;
                        break;
                    }
                }
                if (!found) break;
                string++;
                stringlen--;
            }
        }

        if (dortrim) {
            while (stringlen > 0) {
                char str_ch = string[stringlen - 1];
                bool found = false;
                for (int i = 0; i < setlen; i++) {
                    if (str_ch == set[i]) {
                        found = true;
                        break;
                    }
                }
                if (!found) break;
                stringlen--;
            }
        }
    }

    return cstring_to_text_with_len(string, stringlen);
}
```