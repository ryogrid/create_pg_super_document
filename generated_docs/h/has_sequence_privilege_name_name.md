# has_sequence_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 2108 - 2138

## Overview
Checks user privileges on a sequence given a user name, sequence name, and privilege type, all specified as text/name parameters.

## Definition
```c
Datum has_sequence_privilege_name_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's sequence privilege checking system functions. It verifies whether a specified user has certain privileges on a named sequence. The function takes three parameters: a role name, a sequence name, and a privilege specification string. It performs comprehensive validation to ensure the target object is actually a sequence and not another type of relation.

The function follows PostgreSQL's standard privilege checking workflow:
1. Converts the role name to a role OID, handling both regular roles and the special "public" role
2. Parses the privilege specification string into an internal privilege mode representation
3. Resolves the sequence name to its OID using the table name conversion utility
4. Validates that the resolved object is indeed a sequence (not a table or other relation type)
5. Performs the actual privilege check using PostgreSQL's access control system
6. Returns a boolean result indicating whether the user has the requested privileges

## Parameters / Member Variables
-  (Name): The name of the role/user whose privileges are being checked
-  (text*): The name of the sequence (can be schema-qualified)
-  (text*): Text string specifying the privilege type(s) to check (e.g., "USAGE", "SELECT", "UPDATE")

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public: Converts role name to OID, handling "public" role
  - convert_sequence_priv_string: Converts privilege string to AclMode for sequences
  - convert_table_name: Resolves sequence name to OID (reused from table functions)
  - get_rel_relkind: Retrieves the relation kind to validate it's a sequence
  - pg_class_aclcheck: Performs the actual privilege check on the sequence
  - text_to_cstring: Converts PostgreSQL text to C string for error messages
  - Name, AclResult: Type definitions for PostgreSQL names and ACL results
- Called from (representative examples):
  - This is a system function callable from SQL queries via has_sequence_privilege() function

## Notes and Other Information
- This function is part of PostgreSQL's privilege inquiry system for sequences
- Validates that the named object is actually a sequence using get_rel_relkind()
- Raises an error if the specified object exists but is not a sequence
- Uses the same name resolution mechanism as table functions (convert_table_name)
- The function is typically invoked through SQL function calls rather than direct C code calls
- Part of a family of has_sequence_privilege functions with different parameter combinations
- Located in src/backend/utils/adt/acl.c:2108-2138