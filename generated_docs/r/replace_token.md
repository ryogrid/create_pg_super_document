# replace_token

## Location
[src/bin/initdb/initdb.c:470-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L470-L524)

## Overview
Modifies an array of strings by replacing the first occurrence of a specified token with a replacement string on each line.

## Definition

```c
static char **
replace_token(char **lines, const char *token, const char *replacement)
```
## Detailed Description
This function performs string replacement operations on an array of malloc'd strings, similar to basic sed functionality but without requiring regular expressions. It searches for the first occurrence of a token string in each line of the array and replaces it with the specified replacement string. The function handles memory management by freeing old strings and allocating new ones when replacements change the string length. This is primarily used during PostgreSQL database initialization to customize configuration templates.

## Parameters / Member Variables
- `**lines`: Array of malloc'd strings to be processed, terminated by NULL pointer
- `*token`: The string to search for and replace in each line
- `*replacement`: The string to replace the token with
## Dependencies
- Functions called/Symbols referenced:
  -  (standard library function)
  -  (standard library function) 
  -  (PostgreSQL memory allocation wrapper)
  -  (standard library function)
  -  (standard library function)
  -                total        used        free      shared  buff/cache   available
Mem:        32819380     4786744    25565284        3040     2467352    27650412
Swap:        8388608           0     8388608 (standard library function)
- Called from (representative examples):
  -  (multiple times for various configuration replacements)
  -  (multiple times for template database setup)
  - Used with  macro

## Notes and Other Information
- The function modifies the input array in-place, freeing original strings when replacements occur
- Only replaces the first occurrence of the token on each line
- Efficiently handles size differences between token and replacement strings
- Part of initdb utility's template processing system
- Designed to avoid dependencies on regular expression libraries for simple text substitution

## Simplified Source

```c
static char **replace_token(char **lines, const char *token, const char *replacement)
{
    int toklen, replen, diff;

    toklen = strlen(token);
    replen = strlen(replacement);
    diff = replen - toklen;  // Size difference between replacement and token

    // Process each line in the array
    for (int i = 0; lines[i]; i++)
    {
        char *where;
        char *newline;
        int pre;

        // Find first occurrence of token in this line
        where = strstr(lines[i], token);
        if (where == NULL)
            continue;  // No token found, skip this line

        // Allocate new line with adjusted size
        newline = (char *) pg_malloc(strlen(lines[i]) + diff + 1);

        // Calculate position of token
        pre = where - lines[i];

        // Copy parts: [before token] + [replacement] + [after token]
        memcpy(newline, lines[i], pre);                           // Before token
        memcpy(newline + pre, replacement, replen);               // Replacement
        strcpy(newline + pre + replen, lines[i] + pre + toklen);  // After token

        // Replace old line with new one
        free(lines[i]);
        lines[i] = newline;
    }

    return lines;
}
```