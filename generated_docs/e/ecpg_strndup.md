# ecpg_strndup

## Location
[src/interfaces/ecpg/compatlib/informix.c:179-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L179-L197)

## Overview
A static utility function that creates a null-terminated duplicate of a string with a maximum length limit, providing safe string duplication with length constraints.

## Definition

```c
static char *
ecpg_strndup(const char *str, size_t len)
```
## Detailed Description
The  function is a PostgreSQL ECPG (Embedded SQL in C) compatibility library utility that duplicates a string while enforcing a maximum length constraint. It calculates the minimum between the actual string length and the specified maximum length, then allocates memory for a new string copy. The function ensures proper null-termination and handles memory allocation failures by setting errno to ENOMEM.

This function is part of the Informix compatibility layer in ECPG, providing safe string handling operations that prevent buffer overflows by limiting the copied length.

## Parameters / Member Variables
- `*str`: Source string to be duplicated (const char *)
- `len`: Maximum number of characters to copy from the source string (size_t)
## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - strlen (implicit)
  - memcpy (implicit)
- Called from (representative examples):
  - [deccvasc](../d/deccvasc.md)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Sets errno to ENOMEM on memory allocation failure
- Uses the minimum of the actual string length and the specified maximum length
- Part of the ECPG Informix compatibility library located in src/interfaces/ecpg/compatlib/informix.c
- Static function, only accessible within the same compilation unit
- Handles edge cases where the specified length exceeds the actual string length

## Simplified Source
```c
static char *
ecpg_strndup(const char *str, size_t len)
{
    // Determine actual copy length (minimum of string length and limit)
    size_t real_len = strlen(str);
    int use_len = (real_len > len) ? len : real_len;

    // Allocate memory for new string
    char *new = malloc(use_len + 1);

    if (new) {
        // Copy data and null-terminate
        memcpy(new, str, use_len);
        new[use_len] = '\0';
    } else {
        errno = ENOMEM;
    }

    return new;
}
```