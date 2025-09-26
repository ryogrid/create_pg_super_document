# ENRMetadataGetTupDesc

## Location
src/backend/utils/misc/queryenvironment.c: 125 - 144

## Overview
Gets the TupleDesc for an Ephemeral Named Relation based on which field was filled in its metadata structure.

## Definition

```c
TupleDesc
ENRMetadataGetTupDesc(EphemeralNamedRelationMetadata enrmd)
```
## Detailed Description
This function extracts a TupleDesc (tuple descriptor) from an EphemeralNamedRelationMetadata structure. Ephemeral Named Relations are temporary named relations that don't exist in the system catalogs, such as transition tables in AFTER triggers.

The function handles two different ways that tuple descriptor information can be stored in the metadata:
1. **Direct TupleDesc**: When  is not NULL, it returns this directly stored tuple descriptor
2. **Relation OID reference**: When  contains a valid OID, it opens the referenced relation from the catalog and extracts its tuple descriptor

The function includes an assertion to ensure exactly one of these two fields is populated, as they are mutually exclusive by design. When using catalog relations for the TupleDesc, the function assumes appropriate locks are already held on the relation, as locking at this point would be too late in the processing pipeline.

## Parameters / Member Variables
- : Pointer to EphemeralNamedRelationMetadata structure containing either a direct TupleDesc or a relation OID to derive the TupleDesc from

## Dependencies
- Functions called/Symbols referenced:
  -  (assertion macro)
  -  (opens a table relation)
  -  (closes a table relation)
  -  (constant for invalid OID)
  -  (constant for no locking)
  -  (parameter type)
  -  (return type)

- Called from (representative examples):
  -  (in nodeNamedtuplestorescan.c:111)
  -  (in parse_relation.c:2502)

## Notes and Other Information
- The function assumes that when using catalog relations (), appropriate locks are already held on the relation
- Locking within this function would be too late in the processing sequence
- The function enforces that exactly one of  or  fields must be valid through an assertion
- This is part of PostgreSQL's support for ephemeral named relations, which are primarily used for trigger transition tables
- Located in src/backend/utils/misc/queryenvironment.c (lines 125-144)