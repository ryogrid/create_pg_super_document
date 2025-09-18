# copy_auth_token

## Location
src/backend/libpq/hba.c: 288 - 300

## Overview
A copy constructor function that creates a deep copy of an AuthToken structure by allocating new memory and duplicating the string content and metadata.

## Definition
```c
static AuthToken *copy_auth_token(AuthToken *in)
```

## Detailed Description
The copy_auth_token function creates a complete copy of an existing AuthToken structure. It leverages the make_auth_token function to handle the memory allocation and initialization, passing the original token's string content and quoted flag to create an independent copy.

This function is essential for scenarios where AuthToken structures need to be duplicated, such as when processing HBA configuration lines that may require multiple copies of the same token for different contexts or when building lists of authentication rules.

The copy is completely independent of the original - modifications to either the original or the copy will not affect the other. Note that any regular expression data (regex field) is not copied, as the new token starts with a NULL regex field that can be populated later if needed.

## Parameters / Member Variables
- `in`: Pointer to the source AuthToken structure to be copied

## Dependencies
- Functions called/Symbols referenced:
  - [make_auth_token](../m/make_auth_token.md) (creates the new AuthToken with copied string data)
  - [AuthToken](../A/AuthToken.md) (the struct type being copied)
- Called from (representative examples):
  - [parse_hba_line](../p/parse_hba_line.md) (in src/backend/libpq/hba.c)
  - [parse_ident_line](../p/parse_ident_line.md) (in src/backend/libpq/hba.c)

## Notes and Other Information
- This is a static function, only visible within the hba.c file
- Creates a completely independent copy of the AuthToken
- The regex field is not copied - the new token starts with regex = NULL
- Uses make_auth_token internally, ensuring consistent memory layout and initialization
- Part of PostgreSQL's HBA authentication configuration parsing system
- Useful when the same token needs to be used in multiple contexts or when building authentication rule structures
- The copied token will have the same string content and quoted flag as the original
- Memory for the copy is allocated using PostgreSQL's memory management system (palloc)