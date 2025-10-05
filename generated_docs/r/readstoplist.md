# readstoplist

## Location
[src/backend/tsearch/ts_utils.c:68-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_utils.c#L68-L139)

## Overview
Reads a stop-word file and populates a StopList structure with the words, optionally applying a word processing function to each word before storage.

## Definition

```c
void
readstoplist(const char *fname, StopList *s, char *(*wordop) (const char *))
```
## Detailed Description
This function loads stop words from a specified file into a StopList structure. It handles the complete process of opening the stop-word file (with ".stop" extension), reading each line, processing the words, and storing them in a dynamically allocated array. The function supports optional word preprocessing through a user-supplied function pointer.

The implementation uses PostgreSQL's tsearch_readline facility to read the file line by line, trims trailing whitespace from each word, skips empty lines, and dynamically grows the storage array as needed. After loading all words, the array is sorted using qsort to enable binary search operations for efficient lookups.

## Parameters / Member Variables
- `*fname`: Base filename of the stop-word file (extension ".stop" will be appended automatically)
- `*s`: Pointer to StopList structure to be populated with stop words
- `**wordop`: Optional function pointer for word preprocessing; if NULL, words are stored as-is
## Dependencies
- Functions called/Symbols referenced:
  - [get_tsearch_config_filename](../g/get_tsearch_config_filename.md) (constructs full file path)
  - [tsearch_readline_begin](../t/tsearch_readline_begin.md) (initializes file reading)
  - [tsearch_readline](../t/tsearch_readline.md) (reads each line)
  - [tsearch_readline_end](../t/tsearch_readline_end.md) (cleanup file reading)
  - [t_isspace](../t/t_isspace.md) (checks for whitespace characters)
  - [pg_mblen](../p/pg_mblen.md) (gets multibyte character length)
  - [palloc](../p/palloc.md)/repalloc (PostgreSQL memory allocation)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - qsort (standard sorting function)
  - [pg_qsort_strcmp](../p/pg_qsort_strcmp.md) (PostgreSQL string comparison for qsort)
- Called from (representative examples):
  - [dsnowball_init](../d/dsnowball_init.md) (Snowball dictionary initialization)
  - [dispell_init](../d/dispell_init.md) (Ispell dictionary initialization)
  - [dsimple_init](../d/dsimple_init.md) (Simple dictionary initialization)

## Notes and Other Information
- If no filename is provided (fname is NULL or empty), the StopList will be initialized with zero length
- The function dynamically allocates memory starting with 64 entries and doubles the size when needed
- Words are trimmed of trailing whitespace but leading whitespace is preserved
- Empty lines in the stop-word file are automatically skipped
- The final word list is sorted alphabetically to enable efficient binary search operations
- Memory management follows PostgreSQL conventions using palloc/pfree
- The wordop function allows for custom word transformations (e.g., case normalization, stemming)

## Simplified Source

```c
void
readstoplist(const char *fname, StopList *s, char *(*wordop) (const char *))
{
    char **stop = NULL;
    s->len = 0;

    if (fname && *fname)
    {
        char *filename = get_tsearch_config_filename(fname, "stop");
        tsearch_readline_state trst;
        char *line;
        int reallen = 0;

        // Open stop-word file
        if (!tsearch_readline_begin(&trst, filename))
            ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                           errmsg("could not open stop-word file \"%s\": %m", filename)));

        // Read each line
        while ((line = tsearch_readline(&trst)) != NULL)
        {
            // Trim trailing whitespace
            char *pbuf = line;
            while (*pbuf && !t_isspace(pbuf))
                pbuf += pg_mblen(pbuf);
            *pbuf = '\0';

            // Skip empty lines
            if (*line == '\0')
            {
                pfree(line);
                continue;
            }

            // Expand array if needed
            if (s->len >= reallen)
            {
                reallen = (reallen == 0) ? 64 : reallen * 2;
                stop = (char **) (reallen == 64 ? palloc(sizeof(char *) * reallen) :
                                 repalloc(stop, sizeof(char *) * reallen));
            }

            // Apply word transformation if provided
            if (wordop)
            {
                stop[s->len] = wordop(line);
                if (stop[s->len] != line)
                    pfree(line);
            }
            else
                stop[s->len] = line;

            (s->len)++;
        }

        tsearch_readline_end(&trst);
        pfree(filename);
    }

    s->stop = stop;

    // Sort for binary search
    if (s->stop && s->len > 0)
        qsort(s->stop, s->len, sizeof(char *), pg_qsort_strcmp);
}
```