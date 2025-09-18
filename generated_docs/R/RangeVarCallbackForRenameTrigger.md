# RangeVarCallbackForRenameTrigger

## Location
src/backend/commands/trigger.c: 1416 - 1462

## Overview
A callback function that performs permissions and integrity checks before acquiring a relation lock during trigger rename operations.

## Definition


## Detailed Description
This function serves as a validation callback used during trigger rename operations to ensure that the target relation is appropriate for trigger operations and that the user has sufficient privileges. It performs three main checks: relation kind validation (ensuring only tables, views, foreign tables, and partitioned tables can have triggers), ownership verification (ensuring the user owns the relation), and system catalog protection (preventing modifications to system catalogs unless explicitly allowed).

The function is designed to be called by the RangeVar resolution mechanism before acquiring locks on the target relation, allowing for early detection of permission or compatibility issues.

## Parameters / Member Variables
- : Pointer to RangeVar structure containing the relation name and schema information
- : Object identifier of the resolved relation
- : Previous relation OID (used for detecting concurrent changes)
- : Generic argument pointer (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - errdetail_relkind_not_supported
  - object_ownercheck
  - aclcheck_error
  - get_relkind_objtype
  - get_rel_relkind
  - IsSystemClass
  - ReleaseSysCache
- Called from (representative examples):
  - renametrig

## Notes and Other Information
- Only allows trigger operations on relations of kind RELKIND_RELATION, RELKIND_VIEW, RELKIND_FOREIGN_TABLE, and RELKIND_PARTITIONED_TABLE
- Enforces ownership requirements through object_ownercheck() before allowing trigger modifications
- Respects allowSystemTableMods setting to control system catalog modifications
- Uses system cache lookups for efficient relation metadata access
- Handles concurrent relation drops gracefully by checking tuple validity