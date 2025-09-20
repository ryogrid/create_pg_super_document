# OpfamilynameGetOpfid

## Location
[src/backend/catalog/namespace.c:2223-2255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2223-L2255)

## Overview
Resolves an unqualified operator family name to its OID by searching through the current namespace search path for a given access method.

## Definition

```c
Oid
OpfamilynameGetOpfid(Oid amid, const char *opfname)
```
## Detailed Description
OpfamilynameGetOpfid performs name resolution for operator families in PostgreSQL's namespace system. It searches through the active search path to find an operator family with the specified name that belongs to the given access method (index AM). The function is similar in design to TypenameGetTypid but includes the additional requirement of matching the access method OID.

The function iterates through each namespace in the activeSearchPath, querying the system catalog for an operator family that matches both the name and access method. It excludes temporary namespaces from the search. The first match found determines the result, implementing PostgreSQL's standard name resolution precedence rules.

This function is essential for resolving operator family names in SQL commands and internal operations where only the unqualified name is provided.

## Parameters / Member Variables
- `amid`: The OID of the access method (index AM) that the operator family must belong to
- `opfname`: The unqualified name of the operator family to resolve

## Dependencies
- Functions called/Symbols referenced:
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - GetSysCacheOid3 (using OPFAMILYAMNAMENSP cache)
  - lfirst_oid
  - [ObjectIdGetDatum](ObjectIdGetDatum.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - OidIsValid
- Called from (representative examples):
  - [OpfamilyIsVisibleExt](OpfamilyIsVisibleExt.md) (src/backend/catalog/namespace.c:2308)
  - [OpFamilyCacheLookup](OpFamilyCacheLookup.md) (src/backend/commands/opclasscmds.c:107)

## Notes and Other Information
- Located in src/backend/catalog/namespace.c:2223-2255
- Returns InvalidOid if no matching operator family is found in the search path
- Excludes temporary namespaces (myTempNamespace) from the search
- Uses the OPFAMILYAMNAMENSP system cache for efficient lookup
- Part of PostgreSQL's standard name resolution infrastructure
- The access method constraint distinguishes this from general name resolution functions
- Implements first-match-wins semantics following the search path order