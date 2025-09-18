# RelnameGetRelid

## Location
src/backend/catalog/namespace.c: 885 - 912

## Overview
A simple utility function that resolves an unqualified relation name by searching through the active namespace search path to find the corresponding relation OID.

## Definition
```c
Oid RelnameGetRelid(const char *relname)
```

## Detailed Description
RelnameGetRelid implements the core PostgreSQL namespace search behavior for unqualified relation names. It searches through the active search path (as determined by the search_path setting) in order, checking each namespace for a relation with the specified name.

The function first ensures the namespace search path is current by calling recomputeNamespacePath(), then iterates through each namespace in the activeSearchPath list. It uses get_relname_relid() to check for the relation in each namespace, returning the OID of the first match found.

This is the fundamental mechanism that allows PostgreSQL users to reference tables without fully qualifying them with schema names, following the established search path precedence rules.

## Parameters / Member Variables
- `relname`: The unqualified name of the relation to search for

## Dependencies
- Functions called/Symbols referenced:
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - activeSearchPath (global list)
- Called from (representative examples):
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md)
  - RangeVarGetRelid (header inline function)

## Notes and Other Information
- Returns InvalidOid if the relation is not found in any namespace in the search path
- The search follows the order of namespaces as they appear in the activeSearchPath
- Relies on recomputeNamespacePath() to ensure the search path reflects current session settings
- This is a building block function used by higher-level relation resolution functions
- Does not perform any locking or permission checking - purely a name-to-OID resolution service