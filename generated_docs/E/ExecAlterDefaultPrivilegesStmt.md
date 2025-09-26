# ExecAlterDefaultPrivilegesStmt

## Location
[src/backend/catalog/aclchk.c:976-1160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L976-L1160)

## Overview
Executes ALTER DEFAULT PRIVILEGES statements, which set default access control privileges for objects that will be created in the future by specified roles within specified schemas.

## Definition

```c
struct the "options" part of the statement */
	foreach(cell, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "schemas") == 0)
		{
			if (dnspnames)
				errorConflictingDefElem(defel, pstate);
			dnspnames = defel;
		}
		else if (strcmp(defel->defname, "roles") == 0)
		{
			if (drolespecs)
				errorConflictingDefElem(defel, pstate);
			drolespecs = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized", defel->defname);
	}

	if (dnspnames)
		nspnames = (List *) dnspnames->arg;
```
## Detailed Description
This function implements the ALTER DEFAULT PRIVILEGES SQL command, which allows users to set default access control lists (ACLs) that will be applied to future objects created by specified roles in specified schemas. The function parses the statement's options to extract target schemas and roles, validates privilege specifications for different object types (tables, sequences, functions, procedures, types, schemas), converts role specifications to OIDs, and validates that the current user has sufficient privileges to modify default privileges for the target roles. It uses the InternalDefaultACL structure to represent the parsed statement internally and delegates the actual work to SetDefaultACLsInSchemas for each target role.

## Parameters

- `pstate`: Parse state context containing parsing information and error handling context
- `stmt`: The parsed ALTER DEFAULT PRIVILEGES statement containing action details, target schemas, roles, and privilege specifications

## Dependencies
- Functions called/Symbols referenced:
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md)
  - [get_rolespec_oid](../g/get_rolespec_oid.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [string_to_privilege](../s/string_to_privilege.md)
  - [privilege_to_string](../p/privilege_to_string.md)
  - [GetUserId](../G/GetUserId.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [SetDefaultACLsInSchemas](../S/SetDefaultACLsInSchemas.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- The function handles both scenarios: setting default privileges for the current user (when no roles specified) and for explicitly specified roles (requiring privilege checks)
- It supports various object types with different privilege sets: relations (SELECT, INSERT, UPDATE, DELETE, etc.), sequences (USAGE, SELECT, UPDATE), functions/procedures (EXECUTE), types (USAGE), and schemas (CREATE, USAGE)
- Role specifications are converted to OIDs early in the process, with PUBLIC being converted to ACL_ID_PUBLIC
- Column-level default privileges are explicitly forbidden and will generate an error
- The function validates that specified privileges are valid for the target object type
- Privilege validation ensures the current user has 'privs of role' for any target roles specified
- The actual application of default privileges is delegated to SetDefaultACLsInSchemas for each schema/role combination