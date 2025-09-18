# convert_aclright_to_string

## Location
[src/backend/utils/adt/acl.c:1735-1790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1735-L1790)

## Overview
Converts an individual ACL privilege bit value to its corresponding human-readable string representation.

## Definition
```c
static const char *convert_aclright_to_string(int aclright)
```

## Detailed Description
The `convert_aclright_to_string` function performs the reverse operation of privilege string parsing by converting numeric ACL privilege constants back into their string representations. This function is essential for displaying ACL information in a human-readable format, such as when showing current privileges to database users or administrators.

The function uses a comprehensive switch statement to handle all standard PostgreSQL privilege types including table privileges (INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER), object privileges (EXECUTE, USAGE, CREATE), database privileges (CONNECT, TEMPORARY), and system privileges (SET, ALTER SYSTEM, MAINTAIN). If an unrecognized privilege value is passed, the function raises an error.

## Parameters / Member Variables
- `aclright`: Integer representing a single ACL privilege bit (must be a power of 2 corresponding to one privilege type)

## Dependencies
- Functions called/Symbols referenced:
  - All ACL privilege constants (ACL_INSERT, ACL_SELECT, ACL_UPDATE, etc.)
  - elog (PostgreSQL error logging for unrecognized privileges)
  - ERROR (error level constant)
- Called from (representative examples):
  - [aclexplode](../a/aclexplode.md) (function that breaks down ACL items into their components)

## Notes and Other Information
- This is a static function used internally within the ACL module
- Handles only individual privilege bits, not combinations of privileges
- Returns string constants, not dynamically allocated memory
- Uses the canonical PostgreSQL privilege names as defined in the SQL standard
- Maps ACL_CREATE_TEMP to "TEMPORARY" string for compatibility
- Critical for ACL introspection and debugging functionality
- Throws an error for invalid or unrecognized privilege values to ensure data integrity
- Used primarily by functions that need to display privilege information to users