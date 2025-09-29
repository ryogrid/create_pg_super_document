# replace_variables

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:104-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L104-L158)

## Overview
A static function that processes SQL text to replace named parameters (e.g., :param or ?param) with PostgreSQL-style positional parameters (e.g., $1, $2).

## Definition

```c
static bool
replace_variables(char **text, int lineno)
```
## Detailed Description
The `replace_variables` function transforms SQL statements containing named parameters into PostgreSQL's numbered parameter format. It scans through the input text character by character, identifying parameter markers (: or ?) while properly handling string literals to avoid replacing parameters within quoted strings. When a parameter is found, it replaces the parameter name with a numbered placeholder ($1, $2, etc.) and reallocates the string to accommodate the new format.

## Parameters / Member Variables
- `text`: A double pointer to the SQL text string to be processed; modified in-place with the transformed text
- `lineno`: Line number for error reporting and memory allocation tracking

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - [ecpg_free](../e/ecpg_free.md)
  - [isvarchar](../i/isvarchar.md)
  - snprintf
  - memcpy
  - strcpy
  - strcat
  - strlen
- Called from (representative examples):
  - [prepare_common](../p/prepare_common.md)

## Notes and Other Information
- This is a static function local to the prepare.c file in the ECPG library
- Returns true on success, false on memory allocation failure
- Handles string literals properly by tracking quote state to avoid replacing parameters inside strings
- Skips PostgreSQL's double-colon (::) cast operator to avoid false parameter detection
- Uses a counter to assign sequential numbers to parameters ($1, $2, etc.)
- Performs dynamic memory reallocation to accommodate the text changes
- Handles edge cases where parameter replacement might reach the end of the string
- The function modifies the original text pointer, replacing it with a newly allocated string

## Simplified Source

```c
static bool replace_variables(char **text, int lineno) {
    bool in_string = false;
    int counter = 1;
    int ptr = 0;

    // Scan through the text character by character
    for (; (*text)[ptr] != '\0'; ptr++) {
        // Track if we're inside a string literal
        if ((*text)[ptr] == '\'')
            in_string = !in_string;

        // Skip characters inside strings or non-parameter characters
        if (in_string || ((*text)[ptr] != ':' && (*text)[ptr] != '?'))
            continue;

        // Skip PostgreSQL cast operator '::'
        if ((*text)[ptr] == ':' && (*text)[ptr + 1] == ':') {
            ptr += 2;
        } else {
            // Found a parameter - replace it with $N
            int buffersize = sizeof(int) * CHAR_BIT * 10 / 3;
            char *buffer, *newcopy;
            int len;

            // Create numbered parameter string
            buffer = ecpg_alloc(buffersize, lineno);
            if (!buffer) return false;

            snprintf(buffer, buffersize, "$%d", counter++);

            // Find end of parameter name
            for (len = 1; (*text)[ptr + len] && isvarchar((*text)[ptr + len]); len++);

            // Allocate new string with replacement
            newcopy = ecpg_alloc(strlen(*text) - len + strlen(buffer) + 1, lineno);
            if (!newcopy) {
                ecpg_free(buffer);
                return false;
            }

            // Build new string: before + replacement + after
            memcpy(newcopy, *text, ptr);
            strcpy(newcopy + ptr, buffer);
            strcat(newcopy, (*text) + ptr + len);

            // Clean up and update text pointer
            ecpg_free(*text);
            ecpg_free(buffer);
            *text = newcopy;

            // Adjust pointer for end-of-string case
            if ((*text)[ptr] == '\0')
                ptr--;
        }
    }
    return true;
}
```