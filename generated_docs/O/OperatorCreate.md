-

## Simplified Source

```c
ObjectAddress
OperatorCreate(const char *operatorName,
               Oid operatorNamespace,
               Oid leftTypeId,
               Oid rightTypeId,
               Oid procedureId,
               List *commutatorName,
               List *negatorName,
               Oid restrictionId,
               Oid joinId,
               bool canMerge,
               bool canHash)
{
    Relation pg_operator_desc;
    HeapTuple tup;
    bool isUpdate;
    bool nulls[Natts_pg_operator];
    bool replaces[Natts_pg_operator];
    Datum values[Natts_pg_operator];
    Oid operatorObjectId;
    bool operatorAlreadyDefined;
    Oid operResultType;
    Oid commutatorId, negatorId;
    bool selfCommutator = false;
    NameData oname;
    ObjectAddress address;

    // Validate operator name
    if (!validOperatorName(operatorName))
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                       errmsg("\"%s\" is not a valid operator name", operatorName)));

    // Get function return type and validate parameters
    operResultType = get_func_rettype(procedureId);
    OperatorValidateParams(leftTypeId, rightTypeId, operResultType,
                          commutatorName != NIL, negatorName != NIL,
                          OidIsValid(restrictionId), OidIsValid(joinId),
                          canMerge, canHash);

    // Check if operator already exists or is a shell
    operatorObjectId = OperatorGet(operatorName, operatorNamespace,
                                  leftTypeId, rightTypeId, &operatorAlreadyDefined);

    if (operatorAlreadyDefined)
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_FUNCTION),
                       errmsg("operator %s already exists", operatorName)));

    // Handle shell operator ownership check
    if (OidIsValid(operatorObjectId) &&
        !object_ownercheck(OperatorRelationId, operatorObjectId, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR, operatorName);

    // Set up commutator operator (if specified)
    if (commutatorName) {
        commutatorId = get_other_operator(commutatorName, rightTypeId, leftTypeId,
                                         operatorName, operatorNamespace,
                                         leftTypeId, rightTypeId);

        // Check ownership of commutator
        if (OidIsValid(commutatorId) &&
            !object_ownercheck(OperatorRelationId, commutatorId, GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR,
                          NameListToString(commutatorName));

        if (!OidIsValid(commutatorId))
            selfCommutator = true;
    } else {
        commutatorId = InvalidOid;
    }

    // Set up negator operator (if specified)
    if (negatorName) {
        negatorId = get_other_operator(negatorName, leftTypeId, rightTypeId,
                                      operatorName, operatorNamespace,
                                      leftTypeId, rightTypeId);

        // Check ownership of negator
        if (OidIsValid(negatorId) &&
            !object_ownercheck(OperatorRelationId, negatorId, GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR,
                          NameListToString(negatorName));

        // Prevent self-negation
        if (!OidIsValid(negatorId) || negatorId == operatorObjectId)
            ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                           errmsg("operator cannot be its own negator")));
    } else {
        negatorId = InvalidOid;
    }

    // Initialize tuple data arrays
    for (int i = 0; i < Natts_pg_operator; ++i) {
        values[i] = (Datum) NULL;
        replaces[i] = true;
        nulls[i] = false;
    }

    // Set up operator tuple values
    namestrcpy(&oname, operatorName);
    values[Anum_pg_operator_oprname - 1] = NameGetDatum(&oname);
    values[Anum_pg_operator_oprnamespace - 1] = ObjectIdGetDatum(operatorNamespace);
    values[Anum_pg_operator_oprowner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_pg_operator_oprkind - 1] = CharGetDatum(leftTypeId ? 'b' : 'l');
    values[Anum_pg_operator_oprcanmerge - 1] = BoolGetDatum(canMerge);
    values[Anum_pg_operator_oprcanhash - 1] = BoolGetDatum(canHash);
    values[Anum_pg_operator_oprleft - 1] = ObjectIdGetDatum(leftTypeId);
    values[Anum_pg_operator_oprright - 1] = ObjectIdGetDatum(rightTypeId);
    values[Anum_pg_operator_oprresult - 1] = ObjectIdGetDatum(operResultType);
    values[Anum_pg_operator_oprcom - 1] = ObjectIdGetDatum(commutatorId);
    values[Anum_pg_operator_oprnegate - 1] = ObjectIdGetDatum(negatorId);
    values[Anum_pg_operator_oprcode - 1] = ObjectIdGetDatum(procedureId);
    values[Anum_pg_operator_oprrest - 1] = ObjectIdGetDatum(restrictionId);
    values[Anum_pg_operator_oprjoin - 1] = ObjectIdGetDatum(joinId);

    pg_operator_desc = table_open(OperatorRelationId, RowExclusiveLock);

    // Update existing shell or insert new operator
    if (operatorObjectId) {
        isUpdate = true;
        tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(operatorObjectId));
        if (!HeapTupleIsValid(tup))
            elog(ERROR, "cache lookup failed for operator %u", operatorObjectId);

        replaces[Anum_pg_operator_oid - 1] = false;
        tup = heap_modify_tuple(tup, RelationGetDescr(pg_operator_desc),
                               values, nulls, replaces);
        CatalogTupleUpdate(pg_operator_desc, &tup->t_self, tup);
    } else {
        isUpdate = false;
        operatorObjectId = GetNewOidWithIndex(pg_operator_desc, OperatorOidIndexId,
                                             Anum_pg_operator_oid);
        values[Anum_pg_operator_oid - 1] = ObjectIdGetDatum(operatorObjectId);
        tup = heap_form_tuple(RelationGetDescr(pg_operator_desc), values, nulls);
        CatalogTupleInsert(pg_operator_desc, tup);
    }

    // Add dependencies for the operator
    address = makeOperatorDependencies(tup, true, isUpdate);

    // Handle self-commutator case
    if (selfCommutator)
        commutatorId = operatorObjectId;

    // Update commutator/negator links in related operators
    if (OidIsValid(commutatorId) || OidIsValid(negatorId))
        OperatorUpd(operatorObjectId, commutatorId, negatorId, false);

    // Post-creation hook
    InvokeObjectPostCreateHook(OperatorRelationId, operatorObjectId, 0);

    table_close(pg_operator_desc, RowExclusiveLock);

    return address;
}
```