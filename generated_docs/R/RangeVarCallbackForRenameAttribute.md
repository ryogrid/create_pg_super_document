# RangeVarCallbackForRenameAttribute

## Location
[src/backend/commands/tablecmds.c:3857-3876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3857-L3876)

## Overview
RangeVarCallbackForRenameAttribute is a callback function that performs early permission and integrity validation before acquiring locks during attribute rename operations.

## Definition
```c
static void RangeVarCallbackForRenameAttribute(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg)
```

## Detailed Description
This function serves as a callback for RangeVar processing during attribute rename operations, providing early validation before expensive lock acquisition. It retrieves the relation's metadata from the system cache and delegates to renameatt_check for comprehensive validation. The callback pattern allows for graceful handling of concurrent relation drops and ensures that permission checks occur before potentially blocking lock operations. If the relation is concurrently dropped, the function returns silently.

## Parameters / Member Variables
- `rv`: The RangeVar specifying the relation (unused in implementation)
- `relid`: OID of the target relation for validation
- `oldrelid`: Previous OID in case of relation replacement (unused in implementation)
- `arg`: Additional callback arguments (unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [renameatt_check](../r/renameatt_check.md)
  - [ReleaseSysCache](ReleaseSysCache.md)
  - GETSTRUCT
  - HeapTupleIsValid
- Called from (representative examples):
  - [renameatt](../r/renameatt.md)
  - [RenameConstraint](RenameConstraint.md)

## Notes and Other Information
- Static function used specifically for attribute rename callback validation
- Handles concurrent relation drops gracefully by checking tuple validity
- Delegates actual validation logic to renameatt_check with recursing=false
- Part of PostgreSQL's RangeVar callback mechanism for early validation
- Uses system cache for efficient metadata retrieval during validation
- Does not use several of its parameters, following the standard callback interface pattern

## Simplified Source

```c
static void RangeVarCallbackForRenameAttribute(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg) {
    HeapTuple tuple;
    Form_pg_class form;

    // Get relation information from system cache
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        return; // Relation was concurrently dropped

    // Extract relation form and perform validation checks
    form = (Form_pg_class) GETSTRUCT(tuple);
    renameatt_check(relid, form, false);

    ReleaseSysCache(tuple);
}
```