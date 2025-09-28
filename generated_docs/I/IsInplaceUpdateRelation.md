# IsInplaceUpdateRelation

## Location
[src/backend/catalog/catalog.c:152-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L152-L161)

## Overview
IsInplaceUpdateRelation identifies relations where PostgreSQL core code performs in-place updates, requiring special locking protocols and executor assumptions.

## Definition
```c
bool IsInplaceUpdateRelation(Relation relation)
```

## Detailed Description
This function identifies relations that are subject to in-place updates by PostgreSQL's core code. In-place updates modify tuples directly in their storage location rather than creating new tuple versions, which requires special handling for concurrency control and locking protocols.

The function serves multiple purposes: it's used for assertions to verify code correctness, and it helps the executor implement the proper locking protocol described in README.tuplock section "Locking to write inplace-updated tables". Currently, only pg_class (RelationRelationId) and pg_database (DatabaseRelationId) relations are subject to in-place updates by core PostgreSQL code.

The executor makes important assumptions about these relations: they are not partitions or partitioned tables, and they have no triggers. Extensions may perform in-place updates on other heap tables, but concurrent SQL UPDATE operations may overwrite those modifications.

## Parameters / Member Variables
- `relation`: A Relation structure representing the table/relation to be checked

## Dependencies
- Functions called/Symbols referenced:
  - [IsInplaceUpdateOid](IsInplaceUpdateOid.md)
  - RelationGetRelid (macro to extract OID from relation)
- Called from (representative examples):
  - [check_lock_if_inplace_updateable_rel](../c/check_lock_if_inplace_updateable_rel.md)
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - [CheckValidResultRel](../C/CheckValidResultRel.md)
  - [InitResultRelInfo](InitResultRelInfo.md)

## Notes and Other Information
- Currently only pg_class and pg_database relations support in-place updates in core PostgreSQL
- Used to enforce special locking protocols for concurrent access safety  
- Enables executor optimizations based on assumptions about relation characteristics
- Extensions may perform in-place updates on other tables but risk conflicts with SQL UPDATEs
- Critical for maintaining data consistency in system catalogs that require in-place modification
- The function is located in src/backend/catalog/catalog.c:152-161

## Simplified Source

```c
// Simplified version of IsInplaceUpdateRelation
bool IsInplaceUpdateRelation(Relation relation) {
    // Delegate to OID-based check using relation's OID
    return IsInplaceUpdateOid(RelationGetRelid(relation));
}
```

Key simplifications made:
- This function is already extremely simple - it's just a wrapper around IsInplaceUpdateOid
- The core logic is a single delegation call to check if the relation's OID indicates in-place update capability
- No error handling needed as RelationGetRelid is a simple macro extraction
- The function serves as a convenient interface for callers who have a Relation rather than an OID