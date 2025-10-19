# is_safe_acl_char

## Location
[src/backend/utils/adt/acl.c:142-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L142-L164)

## Overview
Tests whether an identifier character can be left unquoted in Access Control Lists (ACLs), with special handling for high-bit-set characters to maintain compatibility with older PostgreSQL versions.

## Definition

```c
static inline bool
is_safe_acl_char(unsigned char c, bool is_getid)
```
## Detailed Description
This function determines if a character in an identifier can appear without quotes in ACL strings. It implements a compatibility mechanism for handling non-ASCII characters that ensures dump compatibility with old PostgreSQL versions. The function treats high-bit-set characters differently depending on whether it's being called during parsing (getid) or formatting (putid) operations.

For characters with the high bit set (non-ASCII), the function returns the value of  - meaning these characters are always accepted during parsing but may require quoting during output formatting. For ASCII characters, it follows standard identifier rules allowing alphanumeric characters and underscores.

## Parameters / Member Variables
- `c`: The character to test for safety in ACL identifiers
- `is_getid`: Boolean flag indicating if this is called from getid (parsing) context - when true, high-bit-set characters are considered safe
## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to test if character has high bit set)
  - isalnum (standard C library function)
- Called from (representative examples):
  - [getid](../g/getid.md)
  - [putid](../p/putid.md)

## Notes and Other Information
This function addresses a portability issue where older versions used isalnum() on non-ASCII characters, resulting in platform-dependent behavior. The current implementation ensures that dumps created by newer versions remain compatible with older PostgreSQL installations by being more restrictive during output (putid) while remaining permissive during input parsing (getid).

## Simplified Source

```c
static inline bool is_safe_acl_char(unsigned char c, bool is_getid) {
    // High-bit-set characters: safe during parsing, require quoting during output
    if (IS_HIGHBIT_SET(c))
        return is_getid;

    // ASCII characters: allow alphanumeric and underscore
    return isalnum(c) || c == '_';
}
```