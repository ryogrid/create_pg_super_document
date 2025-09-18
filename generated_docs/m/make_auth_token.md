# make_auth_token

## Location
src/backend/libpq/hba.c: 257 - 277

## Overview
A constructor function that creates and initializes an AuthToken struct with a copied string and quoted flag, using a single memory allocation for both the struct and string data.

## Definition
```c
static AuthToken *make_auth_token(const char *token, bool quoted)
```

## Detailed Description
The make_auth_token function constructs a new AuthToken structure by allocating memory for both the struct and the token string in a single palloc block. This memory-efficient approach reduces fragmentation and improves cache locality by storing the struct and its associated string data contiguously in memory.

The function calculates the required memory size (struct size plus string length plus null terminator), allocates the block using palloc0 (which zeros the memory), sets up the string pointer to point to the memory immediately after the struct, copies the input string, and initializes the other fields appropriately.

## Parameters / Member Variables
- `token`: The string to be copied and stored in the AuthToken structure
- `quoted`: Boolean flag indicating whether the original token was quoted in the configuration

## Dependencies
- Functions called/Symbols referenced:
  - strlen (calculates string length)
  - palloc0 (allocates and zeros memory)
  - memcpy (copies the string data)
  - AuthToken (the struct type being created)
- Called from (representative examples):
  - copy_auth_token (in src/backend/libpq/hba.c)
  - next_field_expand (in src/backend/libpq/hba.c)
  - check_ident_usermap (in src/backend/libpq/hba.c)

## Notes and Other Information
- This is a static function, only visible within the hba.c file
- Uses PostgreSQL's memory management system (palloc0) rather than standard malloc
- The memory layout places the AuthToken struct first, followed immediately by the string data
- The 'quoted' field preserves information about whether the token was originally quoted in the configuration file
- The 'regex' field is initialized to NULL and may be populated later if the token is used as a regular expression
- Memory-efficient design reduces allocation overhead and improves performance
- Used in PostgreSQL's HBA authentication system for storing parsed configuration tokens