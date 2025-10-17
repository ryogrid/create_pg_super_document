# dequoteAclUserName

## Location
[src/bin/pg_dump/dumputils.c:616-654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L616-L654)

## Overview
Transfers a user or group name from an input string into an output buffer, dequoting if needed, and returns a pointer to just past the input name.

## Definition

```c
static char *
dequoteAclUserName(PQExpBuffer output, char *input)
```
## Detailed Description
This function processes ACL (Access Control List) user names by extracting them from an input string and placing them into an output buffer with proper dequoting. The function handles both quoted and unquoted user names. For quoted names, it properly processes the PostgreSQL quoting convention where double quotes are escaped as "". The function reads characters until it encounters an unquoted '=' character or reaches the end of the string, which marks the end of the user name portion in ACL entries.

## Parameters / Member Variables
- `output`: PQExpBuffer that will contain the dequoted user name (cleared at start)
- `input`: Input string containing the potentially quoted user name

## Dependencies
- Functions called/Symbols referenced:
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
- Called from (representative examples):
  - [parseAclItem](../p/parseAclItem.md) (src/bin/pg_dump/dumputils.c:436, 448)

## Notes and Other Information
- This is a static function used internally within dumputils.c
- The quoting convention matches the backend's acl.c putid() function
- Handles syntax errors gracefully by returning current position on malformed input
- Used specifically for parsing ACL entries during PostgreSQL dump operations
- The function clears the output buffer before processing, unlike quoteAclUserName()

## Simplified Source

```c
static char *dequoteAclUserName(PQExpBuffer output, char *input) {
    resetPQExpBuffer(output);

    // Process characters until '=' or end of string
    while (*input && *input != '=') {
        if (*input != '"') {
            // Unquoted character - add directly
            appendPQExpBufferChar(output, *input++);
        } else {
            // Quoted name - skip opening quote
            input++;

            // Process until unescaped closing quote
            while (!(*input == '"' && *(input + 1) != '"')) {
                if (*input == '\0')
                    return input;  // Malformed input

                // Handle escaped quotes ("" becomes ")
                if (*input == '"' && *(input + 1) == '"')
                    input++;  // Skip first quote of pair

                appendPQExpBufferChar(output, *input++);
            }
            input++;  // Skip closing quote
        }
    }

    return input;
}
```