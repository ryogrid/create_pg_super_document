# has_sequence_privilege_name

## Location
[src/backend/utils/adt/acl.c:2139-2167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2139-L2167)

## Overview
Checks the current user's privileges on a named sequence given the sequence name and privilege type specification.

## Definition
```c
Datum has_sequence_privilege_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a convenience variant of the sequence privilege checking system that assumes the current user as the subject of the privilege check. It simplifies the common use case where users want to check their own privileges on a sequence without explicitly specifying their role name or ID. The function takes only two parameters: the sequence name and the privilege specification.

Like other sequence privilege functions, it performs validation to ensure the target object is actually a sequence and not another type of relation. The function follows the standard privilege checking workflow but automatically uses the current session's user ID rather than requiring it as a parameter.

The function workflow includes:
1. Automatically determines the current user's OID using GetUserId()
2. Converts the privilege specification string to internal privilege mode representation  
3. Resolves the sequence name to its OID
4. Validates that the resolved object is indeed a sequence
5. Performs the privilege check using PostgreSQL's access control system
6. Returns a boolean result indicating privilege status

## Parameters / Member Variables
-  (text*): The name of the sequence to check privileges on (can be schema-qualified)
-  (text*): Text string specifying the privilege type(s) to check (e.g., "USAGE", "SELECT", "UPDATE")

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md): Returns the OID of the current session user
  - [convert_sequence_priv_string](../c/convert_sequence_priv_string.md): Converts privilege string to AclMode for sequences
  - [convert_table_name](../c/convert_table_name.md): Resolves sequence name to OID (shared with table functions)
  - [get_rel_relkind](../g/get_rel_relkind.md): Retrieves relation kind to validate it's a sequence
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md): Performs the actual privilege check on the sequence
  - text_to_cstring: Converts PostgreSQL text to C string for error messages
  - AclResult: Type definition for ACL check results
- Called from (representative examples):
  - This is a system function callable from SQL queries via has_sequence_privilege() function

## Notes and Other Information
- This function assumes the current user context, making it convenient for self-privilege checks
- Part of PostgreSQL's privilege inquiry system for sequences
- Validates that the named object is actually a sequence, raising an error if it's not
- Uses GetUserId() to automatically determine the current session user
- Simpler interface compared to variants that require explicit role specification
- Part of a family of has_sequence_privilege functions with different parameter combinations
- The function is typically invoked through SQL function calls rather than direct C code calls
- Located in src/backend/utils/adt/acl.c:2139-2167