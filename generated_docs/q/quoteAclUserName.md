# quoteAclUserName

## Location
[src/bin/pg_dump/dumputils.c:582-615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L582-L615)

## Overview
The quoteAclUserName function safely quotes PostgreSQL role/user names for inclusion in ACL strings, following the same quoting rules as the backend's putid() function.

## Definition

```c
void
quoteAclUserName(PQExpBuffer output, const char *input)
```
## Detailed Description
This function ensures that role names are properly quoted when building ACL strings for PostgreSQL dumps. It implements the same identifier quoting logic as the backend's putid() function in acl.c to maintain consistency between the server and client-side ACL handling.

The function performs a two-pass process:
1. First pass: Scans the input string to determine if quoting is necessary
2. Second pass: Copies the string to the output buffer, adding quotes if needed and escaping any embedded quotes

A role name is considered "safe" (not requiring quotes) if it contains only alphanumeric characters and underscores. Any other characters, including spaces, special characters, or SQL keywords, require the name to be double-quoted.

When double quotes are present in the role name itself, they are escaped by doubling them (" becomes "").

## Parameters / Member Variables
- `output`: PQExpBuffer to receive the quoted role name
- `*input`: The unquoted role name string to be processed
## Dependencies
- Functions called/Symbols referenced:
  - isalnum (for checking alphanumeric characters)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md) (for adding characters to the output buffer)

- Called from (representative examples):
  - [getNamespaces](../g/getNamespaces.md) (in pg_dump.c, multiple times for building namespace ACL queries)

## Notes and Other Information
- The function always modifies the output buffer, either with the original string or the quoted version
- Quoting rules must match the backend's putid() function to ensure compatibility
- This function is essential for preventing SQL injection when role names contain special characters
- Double quotes in role names are escaped by doubling them (SQL standard)
- The safety check considers only alphanumeric characters and underscores as safe
- Location: src/bin/pg_dump/dumputils.c:582-615
- This is a public function used across the pg_dump utilities

## Simplified Source

```c
void quoteAclUserName(PQExpBuffer output, const char *input) {
    // Check if the username needs quoting (contains non-alphanumeric chars except underscore)
    bool safe = true;
    for (const char *src = input; *src; src++) {
        if (!isalnum((unsigned char) *src) && *src != '_') {
            safe = false;
            break;
        }
    }

    // Add opening quote if needed
    if (!safe)
        appendPQExpBufferChar(output, '"');

    // Copy username, escaping any double quotes by doubling them
    for (const char *src = input; *src; src++) {
        if (*src == '"')
            appendPQExpBufferChar(output, '"');  // Escape quote
        appendPQExpBufferChar(output, *src);
    }

    // Add closing quote if needed
    if (!safe)
        appendPQExpBufferChar(output, '"');
}
```