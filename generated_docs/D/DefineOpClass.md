# DefineOpClass

## Location
[src/backend/commands/opclasscmds.c:333-771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L333-L771)

## Overview
Creates a new index operator class, which defines how a particular data type can be used with a specific access method by grouping together operators and support functions.

## Definition

```c
ObjectAddress
DefineOpClass(CreateOpClassStmt *stmt)
```
## Detailed Description
DefineOpClass implements the CREATE OPERATOR CLASS SQL command. It creates a new operator class that defines how a specific data type can be indexed using a particular access method. The function validates all components (operators, functions, storage type), ensures proper permissions, creates the necessary catalog entries, and establishes dependency relationships.

Key responsibilities:
- Validates access method existence and retrieves its properties
- Processes and validates operators and support functions
- Handles operator family creation or lookup
- Enforces superuser privilege requirements
- Creates pg_opclass catalog entry
- Establishes dependencies between the opclass and related objects
- Calls access method-specific validation routines
- Stores operators and procedures in pg_amop and pg_amproc catalogs

## Parameters / Member Variables
- : Parsed CREATE OPERATOR CLASS statement containing:
  - : List of names forming the operator class name
  - : Access method name
  - : Data type the operator class applies to
  - : Optional operator family name
  - : List of operators, functions, and storage type specifications
  - : Whether this should be the default opclass for the data type

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [SearchSysCache1](../S/SearchSysCache1.md), SearchSysCache3
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - [superuser](../s/superuser.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - [get_opfamily_oid](../g/get_opfamily_oid.md)
  - [CreateOpFamily](../C/CreateOpFamily.md)
  - [LookupOperWithArgs](../L/LookupOperWithArgs.md), LookupOperName
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [assignOperTypes](../a/assignOperTypes.md), assignProcTypes
  - [addFamilyMember](../a/addFamilyMember.md)
  - [storeOperators](../s/storeOperators.md), storeProcedures
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
  - [EventTriggerCollectCreateOpClass](../E/EventTriggerCollectCreateOpClass.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)

## Notes and Other Information
- Requires superuser privileges due to the complexity of validating operator/function consistency
- Automatically creates an operator family if none is specified and no matching family exists
- Supports three types of items: operators (OPCLASS_ITEM_OPERATOR), support functions (OPCLASS_ITEM_FUNCTION), and storage type (OPCLASS_ITEM_STORAGETYPE)
- Creates hard dependencies from pg_amop and pg_amproc entries to the operator class
- Validates that operator and function numbers are within the access method's supported ranges
- Handles both explicit operator family specification and automatic family creation/lookup
- Storage type specification is optional and validated against access method capabilities

## Simplified Source

```c
ObjectAddress DefineOpClass(CreateOpClassStmt *stmt) {
    char *opcname;
    Oid amoid, typeoid, storageoid, namespaceoid, opfamilyoid, opclassoid;
    int maxOpNumber, maxProcNumber, optsProcNumber;
    List *operators = NIL, *procedures = NIL;
    ObjectAddress myself;

    // Parse qualified name and get namespace
    namespaceoid = QualifiedNameGetCreationNamespace(stmt->opclassname, &opcname);

    // Check creation permissions
    aclresult = object_aclcheck(NamespaceRelationId, namespaceoid, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceoid));

    // Validate access method and get its properties
    tup = SearchSysCache1(AMNAME, CStringGetDatum(stmt->amname));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("access method \"%s\" does not exist", stmt->amname)));

    amform = (Form_pg_am) GETSTRUCT(tup);
    amoid = amform->oid;
    amroutine = GetIndexAmRoutineByAmId(amoid, false);
    ReleaseSysCache(tup);

    maxOpNumber = amroutine->amstrategies;
    maxProcNumber = amroutine->amsupport;
    optsProcNumber = amroutine->amoptsprocnum;

    // Require superuser privileges
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("must be superuser to create an operator class")));

    // Validate data type
    typeoid = typenameTypeId(NULL, stmt->datatype);

    // Find or create operator family
    if (stmt->opfamilyname) {
        opfamilyoid = get_opfamily_oid(amoid, stmt->opfamilyname, false);
    } else {
        // Look for existing family or create new one
        tup = SearchSysCache3(OPFAMILYAMNAMENSP, ObjectIdGetDatum(amoid),
                             PointerGetDatum(opcname), ObjectIdGetDatum(namespaceoid));
        if (HeapTupleIsValid(tup)) {
            opfamilyoid = ((Form_pg_opfamily) GETSTRUCT(tup))->oid;
            ReleaseSysCache(tup);
        } else {
            // Create new operator family
            CreateOpFamilyStmt *opfstmt = makeNode(CreateOpFamilyStmt);
            opfstmt->opfamilyname = stmt->opclassname;
            opfstmt->amname = stmt->amname;
            ObjectAddress tmpAddr = CreateOpFamily(opfstmt, opcname, namespaceoid, amoid);
            opfamilyoid = tmpAddr.objectId;
        }
    }

    storageoid = InvalidOid;

    // Process operators, functions, and storage type from statement items
    foreach(l, stmt->items) {
        CreateOpClassItem *item = lfirst_node(CreateOpClassItem, l);
        OpFamilyMember *member;

        switch (item->itemtype) {
            case OPCLASS_ITEM_OPERATOR:
                // Validate operator number range
                if (item->number <= 0 || item->number > maxOpNumber)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errmsg("invalid operator number %d", item->number)));

                // Lookup operator and add to list
                operOid = (item->name->objargs != NIL) ?
                         LookupOperWithArgs(item->name, false) :
                         LookupOperName(NULL, item->name->objname, typeoid, typeoid, false, -1);

                member = palloc0(sizeof(OpFamilyMember));
                member->is_func = false;
                member->object = operOid;
                member->number = item->number;
                assignOperTypes(member, amoid, typeoid);
                addFamilyMember(&operators, member);
                break;

            case OPCLASS_ITEM_FUNCTION:
                // Validate function number range
                if (item->number <= 0 || item->number > maxProcNumber)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errmsg("invalid function number %d", item->number)));

                // Lookup function and add to list
                funcOid = LookupFuncWithArgs(OBJECT_FUNCTION, item->name, false);
                member = palloc0(sizeof(OpFamilyMember));
                member->is_func = true;
                member->object = funcOid;
                member->number = item->number;
                assignProcTypes(member, amoid, typeoid, optsProcNumber);
                addFamilyMember(&procedures, member);
                break;

            case OPCLASS_ITEM_STORAGETYPE:
                if (OidIsValid(storageoid))
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errmsg("storage type specified more than once")));
                storageoid = typenameTypeId(NULL, item->storedtype);
                break;
        }
    }

    // Validate storage type specification
    if (OidIsValid(storageoid)) {
        if (storageoid == typeoid)
            storageoid = InvalidOid;
        else if (!amroutine->amstorage)
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                           errmsg("storage type cannot be different from data type")));
    }

    // Check for duplicate operator class
    rel = table_open(OperatorClassRelationId, RowExclusiveLock);
    if (SearchSysCacheExists3(CLAAMNAMENSP, ObjectIdGetDatum(amoid),
                             CStringGetDatum(opcname), ObjectIdGetDatum(namespaceoid)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("operator class \"%s\" for access method \"%s\" already exists",
                              opcname, stmt->amname)));

    // Check for conflicting default operator class if this is default
    if (stmt->isDefault) {
        // Scan for existing default opclass for this type and access method
        // Error if found
    }

    // Create pg_opclass entry
    opclassoid = GetNewOidWithIndex(rel, OpclassOidIndexId, Anum_pg_opclass_oid);
    // Fill values array with opclass data
    // Insert tuple into catalog

    // Set up dependency info for operators and procedures
    foreach(l, operators) {
        OpFamilyMember *op = (OpFamilyMember *) lfirst(l);
        op->ref_is_hard = true;
        op->ref_is_family = false;
        op->refobjid = opclassoid;
    }
    foreach(l, procedures) {
        OpFamilyMember *proc = (OpFamilyMember *) lfirst(l);
        proc->ref_is_hard = true;
        proc->ref_is_family = false;
        proc->refobjid = opclassoid;
    }

    // Let access method validate and adjust member lists
    if (amroutine->amadjustmembers)
        amroutine->amadjustmembers(opfamilyoid, opclassoid, operators, procedures);

    // Store operators and procedures in pg_amop and pg_amproc
    storeOperators(stmt->opfamilyname, amoid, opfamilyoid, operators, false);
    storeProcedures(stmt->opfamilyname, amoid, opfamilyoid, procedures, false);

    // Create dependencies
    myself.classId = OperatorClassRelationId;
    myself.objectId = opclassoid;
    myself.objectSubId = 0;

    // Dependencies on namespace, opfamily, data type, storage type, owner, extension
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);  // namespace
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);    // opfamily
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);  // data type
    if (OidIsValid(storageoid))
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);  // storage type
    recordDependencyOnOwner(OperatorClassRelationId, opclassoid, GetUserId());
    recordDependencyOnCurrentExtension(&myself, false);

    // Fire event triggers and post-creation hooks
    EventTriggerCollectCreateOpClass(stmt, opclassoid, operators, procedures);
    InvokeObjectPostCreateHook(OperatorClassRelationId, opclassoid, 0);

    table_close(rel, RowExclusiveLock);
    return myself;
}
```