# get_previous_words

## Location
[src/bin/psql/tab-complete.c:6253-6379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L6253-L6379)

## Overview
Parses all words before the cursor position in psql's command line, returning them in reverse order for tab completion context analysis.

## Definition
```c
static char **
get_previous_words(int point, char **buffer, int *nwords)
```

## Detailed Description
The `get_previous_words` function performs sophisticated parsing of the command line text preceding the cursor position. It handles complex SQL syntax including quoted identifiers, parentheses, and multi-line queries. The function works by:

1. **Query Construction**: Combines any existing query buffer (`tab_completion_query_buf`) with the current line buffer to handle multi-line SQL statements
2. **Memory Allocation**: Pre-allocates arrays for word pointers and string storage, using worst-case estimates for efficiency
3. **Backward Parsing**: Scans from the cursor position backward, identifying word boundaries while respecting SQL syntax rules
4. **Quote Handling**: Properly handles double-quoted identifiers, treating quoted content as single words
5. **Parentheses Tracking**: Maintains parentheses balance to correctly identify word boundaries within function calls and expressions
6. **Word Extraction**: Copies identified words into the output buffer, maintaining reverse chronological order

The parsing uses WORD_BREAKS characters to identify word boundaries but includes sophisticated logic to handle SQL-specific cases like quoted identifiers and nested expressions.

## Parameters / Member Variables
- `point`: The cursor position in the input line (character offset)
- `buffer`: Pointer to receive the allocated string buffer (output parameter)
- `nwords`: Pointer to receive the number of words found (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (multiple calls for memory allocation)
  - WORD_BREAKS (constant defining word-breaking characters)
  - tab_completion_query_buf (global buffer for multi-line queries)
  - rl_line_buffer (readline's current line buffer)
- Called from (representative examples):
  - THING_NO_SHOW (completion handling)
  - HeadMatchesCS (case-sensitive header matching for completion)

## Notes and Other Information
- Returns malloc'd array of character pointers in reverse order (most recent word first)
- Caller must free both the returned array and the buffer pointed to by *buffer
- Part of psql's tab completion system in PostgreSQL
- Located in src/bin/psql/tab-complete.c at lines 6253-6379
- The function is static, meaning it's only accessible within the tab-complete.c file
- Handles multi-line SQL queries by combining query buffer with current line
- Sophisticated parsing includes quote and parentheses handling for SQL syntax
- Words are returned right-to-left: previous_words[0] is the most recent word
- Optimized memory allocation using worst-case estimates to avoid multiple malloc calls

## Simplified Source

```c
static char **
get_previous_words(int point, char **buffer, int *nwords)
{
    char **previous_words;
    char *buf;
    char *outptr;
    int words_found = 0;
    int i;

    // Construct full query by combining query buffer with current line
    if (tab_completion_query_buf && tab_completion_query_buf->len > 0) {
        i = tab_completion_query_buf->len;
        buf = pg_malloc(point + i + 2);
        memcpy(buf, tab_completion_query_buf->data, i);
        buf[i++] = '\n';
        memcpy(buf + i, rl_line_buffer, point);
        i += point;
        buf[i] = '\0';
        point = i;
    } else {
        buf = rl_line_buffer;
    }

    // Allocate arrays for words and string storage
    previous_words = (char **) pg_malloc(point * sizeof(char *));
    *buffer = outptr = (char *) pg_malloc(point * 2);

    // Skip any non-word character at cursor position
    for (i = point - 1; i >= 0; i--) {
        if (strchr(WORD_BREAKS, buf[i]))
            break;
    }
    point = i;

    // Parse words backward from cursor position
    while (point >= 0) {
        int start, end;
        bool inquotes = false;
        int parentheses = 0;

        // Find end of current word (first non-space going backward)
        end = -1;
        for (i = point; i >= 0; i--) {
            if (!isspace((unsigned char) buf[i])) {
                end = i;
                break;
            }
        }
        if (end < 0)
            break;

        // Find start of word, handling quotes and parentheses
        for (start = end; start > 0; start--) {
            if (buf[start] == '"')
                inquotes = !inquotes;
            if (!inquotes) {
                if (buf[start] == ')')
                    parentheses++;
                else if (buf[start] == '(') {
                    if (--parentheses <= 0)
                        break;
                } else if (parentheses == 0 && strchr(WORD_BREAKS, buf[start - 1])) {
                    break;
                }
            }
        }

        // Copy the word to output buffer
        previous_words[words_found++] = outptr;
        i = end - start + 1;
        memcpy(outptr, &buf[start], i);
        outptr += i;
        *outptr++ = '\0';

        // Continue with next word
        point = start - 1;
    }

    // Clean up temporary buffer if we created one
    if (buf != rl_line_buffer)
        free(buf);

    *nwords = words_found;
    return previous_words;
}
```