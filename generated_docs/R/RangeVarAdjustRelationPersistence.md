# RangeVarAdjustRelationPersistence

## Location
[src/backend/catalog/namespace.c:846-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L846-L884)

## Overview
A utility function that validates and adjusts the persistence attribute of a RangeVar based on the target namespace, ensuring consistency between relation persistence and schema type.

## Definition
```c
void RangeVarAdjustRelationPersistence(RangeVar *newRelation, Oid nspid)
```

## Detailed Description
RangeVarAdjustRelationPersistence enforces the rules governing the relationship between relation persistence (temporary vs permanent) and the type of namespace where the relation will be created. It validates that the combination is legal and automatically adjusts the persistence when appropriate.

The function handles three main cases:
1. RELPERSISTENCE_TEMP relations must be created in appropriate temporary namespaces
2. RELPERSISTENCE_PERMANENT relations are automatically converted to temporary when created in current session's temporary namespace
3. Relations with other persistence types (like unlogged) cannot be created in any temporary namespace

The function prevents creation of relations in temporary namespaces belonging to other sessions, which would be invalid and potentially dangerous.

## Parameters / Member Variables
- `newRelation`: RangeVar structure that may be modified to adjust the relpersistence field based on the target namespace
- `nspid`: OID of the target namespace where the relation will be created

## Dependencies
- Functions called/Symbols referenced:
  - [isTempOrTempToastNamespace](../i/isTempOrTempToastNamespace.md)
  - [isAnyTempNamespace](../i/isAnyTempNamespace.md)
  - RELPERSISTENCE_TEMP
  - RELPERSISTENCE_PERMANENT
- Called from (representative examples):
  - [RangeVarGetAndCheckCreationNamespace](RangeVarGetAndCheckCreationNamespace.md)
  - [DefineCompositeType](../D/DefineCompositeType.md)
  - [generateSerialExtraStmts](../g/generateSerialExtraStmts.md)

## Notes and Other Information
- Modifies the input RangeVar structure in-place when persistence adjustment is needed
- Automatically converts permanent relations to temporary when created in the current session's temp namespace
- Raises errors for invalid combinations rather than attempting automatic corrections in most cases
- Prevents cross-session temporary namespace access which could lead to security issues
- Critical for maintaining the integrity of the temporary table system in PostgreSQL

## Simplified Source

```c
void RangeVarAdjustRelationPersistence(RangeVar *newRelation, Oid nspid) {
    switch (newRelation->relpersistence) {
        case RELPERSISTENCE_TEMP:
            // Temporary relations must be in appropriate temp namespaces
            if (!isTempOrTempToastNamespace(nspid)) {
                if (isAnyTempNamespace(nspid)) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                   errmsg("cannot create relations in temporary schemas of other sessions")));
                } else {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                   errmsg("cannot create temporary relation in non-temporary schema")));
                }
            }
            break;

        case RELPERSISTENCE_PERMANENT:
            // Auto-convert permanent to temp if in our temp namespace
            if (isTempOrTempToastNamespace(nspid)) {
                newRelation->relpersistence = RELPERSISTENCE_TEMP;
            } else if (isAnyTempNamespace(nspid)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                               errmsg("cannot create relations in temporary schemas of other sessions")));
            }
            break;

        default:
            // Other persistence types (unlogged, etc.) not allowed in temp schemas
            if (isAnyTempNamespace(nspid)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                               errmsg("only temporary relations may be created in temporary schemas")));
            }
    }
}
```