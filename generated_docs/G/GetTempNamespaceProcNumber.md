# GetTempNamespaceProcNumber

## Location
src/backend/catalog/namespace.c: 3766 - 3790

## Overview
This function extracts the process number from a temporary namespace name by parsing the numeric suffix in temporary table or temporary toast table namespace names.

## Definition
```c
ProcNumber GetTempNamespaceProcNumber(Oid namespaceId)
```

## Detailed Description
The function determines which backend process owns a temporary namespace by extracting the process number embedded in the namespace name. PostgreSQL temporary namespaces follow naming conventions where the process number is appended to standard prefixes:
- Regular temporary namespaces: "pg_temp_[procnumber]"
- Temporary toast namespaces: "pg_toast_temp_[procnumber]"

The function retrieves the namespace name using `get_namespace_name()`, checks for the appropriate prefixes, and then uses `atoi()` to parse the numeric suffix as the process number. If the namespace doesn't match either pattern or doesn't exist, it returns `INVALID_PROC_NUMBER`.

## Parameters / Member Variables
- `namespaceId`: The OID of the namespace to extract the process number from

## Dependencies
- Functions called/Symbols referenced:
  - get_namespace_name
  - strncmp (standard C library function)
  - atoi (standard C library function)
  - pfree (PostgreSQL memory management)
  - INVALID_PROC_NUMBER (constant)

- Called from (representative examples):
  - checkTempNamespaceStatus
  - pg_relation_filepath
  - RelationBuildDesc
  - RangeVarGetRelid

## Notes and Other Information
- Returns `INVALID_PROC_NUMBER` if the namespace is not a temporary namespace or doesn't exist
- Handles both regular temporary table namespaces and temporary toast table namespaces
- The extracted process number corresponds to the backend session that owns the temporary namespace
- Used extensively in relation cache building and temporary object management
- The function assumes that the numeric suffix in the namespace name directly corresponds to a valid process number
- Critical for identifying which backend owns temporary objects for cleanup and access control purposes