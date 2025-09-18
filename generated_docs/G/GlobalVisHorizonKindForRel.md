# GlobalVisHorizonKindForRel

## Location
src/backend/storage/ipc/procarray.c: 1971 - 2004

## Overview
GlobalVisHorizonKindForRel determines the appropriate visibility horizon kind for a given relation, helping PostgreSQL decide which vacuum and transaction cleanup strategy to apply based on the relation type and system state.

## Definition
```c
static inline GlobalVisHorizonKind
GlobalVisHorizonKindForRel(Relation rel)
```

## Detailed Description
This function categorizes relations into different visibility horizon types to optimize vacuum and transaction cleanup operations. It analyzes the relation properties and current system state to return one of four horizon kinds:

- **VISHORIZON_SHARED**: Used for shared relations, NULL relations, or during recovery
- **VISHORIZON_CATALOG**: Used for catalog relations and relations accessible in logical decoding
- **VISHORIZON_DATA**: Used for regular user data relations
- **VISHORIZON_TEMP**: Used for temporary local relations

The function implements a hierarchical decision tree, checking from most conservative (shared) to least conservative (temp) visibility requirements. It ensures that relations requiring stricter cleanup policies (like system catalogs) receive appropriate treatment.

## Parameters / Member Variables
- `rel`: Relation pointer to analyze; if NULL, the most conservative horizon (VISHORIZON_SHARED) is returned

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress
  - IsCatalogRelation
  - RelationIsAccessibleInLogicalDecoding
  - RELATION_IS_LOCAL
  - RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_TOASTVALUE (constants)
  - VISHORIZON_SHARED, VISHORIZON_CATALOG, VISHORIZON_DATA, VISHORIZON_TEMP (enum values)
- Called from:
  - GetOldestNonRemovableTransactionId
  - GlobalVisTestFor

## Notes and Other Information
- The function is marked as `static inline` for performance optimization since it's called frequently during vacuum operations
- Contains an assertion to ensure only supported relation kinds (regular relations, materialized views, and TOAST values) are processed
- The decision logic prioritizes safety: shared relations and recovery mode always use the most conservative horizon
- Temporary relations get the most aggressive cleanup since they're session-local and don't need to consider other transactions