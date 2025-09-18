# hashadjustmembers

## Location
src/backend/access/hash/hashvalidate.c: 352 - 439

## Overview
The hashadjustmembers function is a prechecking function that determines appropriate dependency relationships (hard vs soft, opclass vs opfamily) when adding operators and functions to a hash operator family.

## Definition
```c
void hashadjustmembers(Oid opfamilyoid, Oid opclassoid, List *operators, List *functions)
```

## Detailed Description
This function implements the dependency management logic for PostgreSQL's hash access method when operators or support functions are added to an operator family. It determines whether each new member should have:

1. **Hard vs Soft Dependencies**: Hard dependencies prevent deletion of the referenced object, while soft dependencies allow it.
2. **Opclass vs Opfamily References**: Members can be tied to a specific operator class or to the broader operator family.

The function applies these rules:
- **Optional support functions** (not HASHSTANDARD_PROC): Always soft family dependencies
- **Cross-type operators/functions**: Always soft family dependencies  
- **Same-type operators/functions**: Hard opclass dependencies if a suitable opclass exists, otherwise soft family dependencies

This logic handles dump/reload scenarios and prevents creation of incomplete operator classes while maintaining proper dependency relationships for hash access method structures.

## Parameters / Member Variables
- `opfamilyoid`: The OID of the hash operator family being modified
- `opclassoid`: The OID of the operator class (if any) in the context of which members are being added
- `operators`: List of OpFamilyMember structures representing operators to add
- `functions`: List of OpFamilyMember structures representing support functions to add

## Dependencies
- Functions called/Symbols referenced:
  - CommandCounterIncrement
  - [get_opclass_input_type](../g/get_opclass_input_type.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [opclass_for_family_datatype](../o/opclass_for_family_datatype.md)
  - OidIsValid
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (in hash access method interface)

## Notes and Other Information
- The function implements caching of opclass lookups to avoid expensive repeated searches for the same data type.
- During CREATE OPERATOR CLASS operations, CommandCounterIncrement() is called to ensure visibility of the new pg_opclass row.
- The dependency choices made here can affect dump/reload behavior, but pg_dump's existing logic preserves most dependency relationships correctly.
- Cross-type operators that were incorrectly bound tightly to an opclass will be "silently fixed" to use soft family dependencies.
- Located in src/backend/access/hash/hashvalidate.c:352-439.