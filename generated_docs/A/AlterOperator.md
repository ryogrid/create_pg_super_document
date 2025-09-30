# AlterOperator

## Location
[src/backend/commands/operatorcmds.c:462-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/operatorcmds.c#L462-L701)

## Overview
Implements the `ALTER OPERATOR` SQL command to modify specific attributes of existing operators, including restriction/join estimators and operator properties like commutator, negator, merges, and hashes.

## Definition
```c
ObjectAddress AlterOperator(AlterOperatorStmt *stmt)
```

## Detailed Description
The `AlterOperator` function processes `ALTER OPERATOR <operator> SET (option = ...)` statements to modify existing operator definitions in the PostgreSQL catalog. The function enforces strict rules about which attributes can be changed to maintain system integrity and prevent invalidation of existing query plans.

Currently supported modifications include:
- **RESTRICT and JOIN estimator functions**: Can be changed or removed at any time
- **COMMUTATOR, NEGATOR, MERGES, and HASHES attributes**: Can only be set if they were not previously defined (to prevent plan invalidation)

The function performs comprehensive validation of the requested changes, updates the `pg_operator` catalog, maintains dependency relationships, and ensures bidirectional consistency for commutator and negator relationships.

## Parameters / Member Variables
- `stmt`: Pointer to `AlterOperatorStmt` structure containing:
  - `opername`: The operator name and argument types to be modified
  - `options`: List of `DefElem` structures specifying the attributes to change

## Dependencies
- Functions called/Symbols referenced:
  - [LookupOperWithArgs](../L/LookupOperWithArgs.md): Resolves operator name to OID
  - `SearchSysCacheCopy1`: Retrieves operator tuple from system catalog
  - [defGetQualifiedName](../d/defGetQualifiedName.md): Extracts qualified names from DefElem options
  - [defGetBoolean](../d/defGetBoolean.md): Extracts boolean values from DefElem options  
  - [object_ownercheck](../o/object_ownercheck.md): Verifies user ownership permissions
  - [ValidateRestrictionEstimator](../V/ValidateRestrictionEstimator.md): Validates restriction selectivity estimator function
  - [ValidateJoinEstimator](../V/ValidateJoinEstimator.md): Validates join selectivity estimator function
  - [ValidateOperatorReference](../V/ValidateOperatorReference.md): Validates commutator/negator operator references
  - [OperatorValidateParams](../O/OperatorValidateParams.md): Performs logical consistency validation
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates modified catalog tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the catalog with changes
  - [makeOperatorDependencies](../m/makeOperatorDependencies.md): Updates dependency relationships
  - [OperatorUpd](../O/OperatorUpd.md): Updates back-references in related operators
  - `InvokeObjectPostAlterHook`: Triggers post-alter event hooks

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processing entry point

## Notes and Other Information
- **Security**: Requires ownership of the operator being modified
- **Immutable attributes**: Function, leftarg, rightarg, and procedure cannot be changed after creation
- **Plan stability**: Commutator, negator, merges, and hashes attributes can only be set once to prevent invalidation of cached query plans
- **Validation**: All changes undergo the same validation as operator creation via `OperatorValidateParams`
- **Bidirectional consistency**: When commutator or negator relationships are established, both operators are updated via `OperatorUpd`
- **Self-reference protection**: Prevents operators from being their own negator
- **Dependency management**: Automatically updates object dependencies when relationships change

## Simplified Source

```c
ObjectAddress
AlterOperator(AlterOperatorStmt *stmt)
{
    ObjectAddress address;
    Oid oprId;
    Relation catalog;
    HeapTuple tup;
    Form_pg_operator oprForm;
    Datum values[Natts_pg_operator];
    bool nulls[Natts_pg_operator], replaces[Natts_pg_operator];

    Oid restrictionOid = InvalidOid, joinOid = InvalidOid;
    Oid commutatorOid = InvalidOid, negatorOid = InvalidOid;
    bool canMerge = false, canHash = false;
    bool updateRestriction = false, updateJoin = false;
    bool updateMerges = false, updateHashes = false;

    // Look up the operator
    oprId = LookupOperWithArgs(stmt->opername, false);
    catalog = table_open(OperatorRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(oprId));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for operator %u", oprId);
    oprForm = (Form_pg_operator) GETSTRUCT(tup);

    // Process option changes
    foreach(pl, stmt->options) {
        DefElem *defel = (DefElem *) lfirst(pl);

        if (strcmp(defel->defname, "restrict") == 0) {
            restrictionName = defel->arg ? defGetQualifiedName(defel) : NIL;
            updateRestriction = true;
        } else if (strcmp(defel->defname, "join") == 0) {
            joinName = defel->arg ? defGetQualifiedName(defel) : NIL;
            updateJoin = true;
        } else if (strcmp(defel->defname, "commutator") == 0) {
            commutatorName = defGetQualifiedName(defel);
        } else if (strcmp(defel->defname, "negator") == 0) {
            negatorName = defGetQualifiedName(defel);
        } else if (strcmp(defel->defname, "merges") == 0) {
            canMerge = defGetBoolean(defel);
            updateMerges = true;
        } else if (strcmp(defel->defname, "hashes") == 0) {
            canHash = defGetBoolean(defel);
            updateHashes = true;
        }
    }

    // Check permissions (must be owner)
    if (!object_ownercheck(OperatorRelationId, oprId, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR, NameStr(oprForm->oprname));

    // Validate function references
    if (restrictionName)
        restrictionOid = ValidateRestrictionEstimator(restrictionName);
    if (joinName)
        joinOid = ValidateJoinEstimator(joinName);
    if (commutatorName)
        commutatorOid = ValidateOperatorReference(commutatorName, oprForm->oprright, oprForm->oprleft);
    if (negatorName) {
        negatorOid = ValidateOperatorReference(negatorName, oprForm->oprleft, oprForm->oprright);
        if (negatorOid == oprForm->oid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                           errmsg("operator cannot be its own negator")));
    }

    // Prevent changing attributes that might invalidate plans
    if (OidIsValid(commutatorOid) && OidIsValid(oprForm->oprcom) &&
        commutatorOid != oprForm->oprcom)
        ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                       errmsg("operator attribute \"commutator\" cannot be changed if already set")));

    // Build updated tuple
    memset(values, 0, sizeof(values));
    memset(replaces, false, sizeof(replaces));
    memset(nulls, false, sizeof(nulls));

    if (updateRestriction) {
        replaces[Anum_pg_operator_oprrest - 1] = true;
        values[Anum_pg_operator_oprrest - 1] = ObjectIdGetDatum(restrictionOid);
    }
    // Similar updates for other modified attributes...

    tup = heap_modify_tuple(tup, RelationGetDescr(catalog), values, nulls, replaces);
    CatalogTupleUpdate(catalog, &tup->t_self, tup);

    address = makeOperatorDependencies(tup, false, true);

    if (OidIsValid(commutatorOid) || OidIsValid(negatorOid))
        OperatorUpd(oprId, commutatorOid, negatorOid, false);

    InvokeObjectPostAlterHook(OperatorRelationId, oprId, 0);
    table_close(catalog, NoLock);

    return address;
}
```