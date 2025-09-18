# validate_relation_kind

## Location
[src/backend/access/sequence/sequence.c:70-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/sequence/sequence.c#L70-L78)

## Overview
A static inline validation function that ensures a relation is a sequence, raising an error if the relation kind is incorrect.

## Definition
```c
static inline void validate_relation_kind(Relation r)
```

## Detailed Description
The `validate_relation_kind` function is a type-safety mechanism used within the sequence access subsystem to validate that a given relation is actually a sequence. This function checks the `relkind` field of the relation's tuple descriptor to ensure it matches `RELKIND_SEQUENCE`. If the relation is not a sequence, the function raises a detailed error message indicating the relation name and the actual relation kind that was encountered.

This validation function is critical for maintaining data integrity and preventing incorrect operations on non-sequence relations. It serves as a runtime type check that complements PostgreSQL's static type system. Note that there are similar `validate_relation_kind` functions in other subsystems (such as indexam.c for indexes) that perform analogous validation for their respective relation types.

## Parameters / Member Variables
- `r`: The relation to validate, must be a sequence relation

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_SEQUENCE (constant)
  - ereport
  - [errcode](../e/errcode.md)
  - ERRCODE_WRONG_OBJECT_TYPE
  - [errmsg](../e/errmsg.md)
  - RelationGetRelationName
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
- Called from (representative examples):
  - [sequence_open](../s/sequence_open.md)

## Notes and Other Information
- This function is declared as `static inline` for performance optimization, as it's a simple validation that may be called frequently
- Located in src/backend/access/sequence/sequence.c, specifically for sequence relation validation
- There are other functions with the same name in different files (e.g., indexam.c) that validate different relation types
- The error reporting is comprehensive, providing both the relation name and details about the unsupported relation kind
- This function is part of PostgreSQL's defense-in-depth approach to type safety and data integrity