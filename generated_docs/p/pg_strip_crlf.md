# pg_strip_crlf

## Location
[src/common/string.c:155-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/string.c#L155-L164)

## Overview
A utility function that removes trailing newline and carriage return characters from a string in-place, commonly used for cleaning up file input and command output.

## Definition
```c
int pg_strip_crlf(char *str)
```

## Detailed Description
This function modifies the input string by removing any trailing '\n' (newline) and '\r' (carriage return) characters, which is particularly useful for processing text that comes from files, command output, or user input. The function works backwards from the end of the string, null-terminating it at the first position that doesn't contain a newline or carriage return character.

The function handles both Unix-style line endings ('\n') and Windows-style line endings ('\r\n' or just '\r'), making it cross-platform compatible. It modifies the original string in-place and returns the new length, providing both string modification and length information in a single operation.

## Parameters / Member Variables
- `*str`: The null-terminated input string to be modified by removing trailing CRLF characters
## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
- Called from (representative examples):
  - [run_ssl_passphrase_command](../r/run_ssl_passphrase_command.md) (src/backend/libpq/be-secure-common.c:102)
  - [tokenize_auth_file](../t/tokenize_auth_file.md) (src/backend/libpq/hba.c:736)
  - [simple_prompt_extended](../s/simple_prompt_extended.md) (src/common/sprompt.c:152)
  - [passwordFromFile](passwordFromFile.md) (src/interfaces/libpq/fe-connect.c:7511)
  - [get_su_pwd](../g/get_su_pwd.md) (src/bin/initdb/initdb.c:1688)

## Notes and Other Information
- Modifies the input string in-place rather than creating a copy
- Returns the new string length after stripping, which can be more efficient than calling  again
- Handles multiple trailing newlines and carriage returns by continuing to strip until no more are found
- Commonly used when reading configuration files, command output, and user passwords where trailing whitespace should be removed
- Essential for processing authentication files and SSL passphrase commands where clean input is required
- The backwards iteration approach efficiently handles strings with multiple trailing line terminators

## Simplified Source

```c
// Simplified version of pg_strip_crlf
int pg_strip_crlf(char *str) {
    // Get the current length of the string
    int len = strlen(str);

    // Remove trailing newlines and carriage returns by working backwards
    while (len > 0 && (str[len - 1] == '\n' || str[len - 1] == '\r')) {
        str[--len] = '\0';  // Null-terminate at the new position
    }

    // Return the new length after stripping
    return len;
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Maintained the core algorithm without changes since it's already quite simple
- Preserved the efficient backwards iteration approach
- Kept the in-place modification behavior which is essential to the function's purpose