# brinvalidate

## Location
[src/backend/access/brin/brin_validate.c:37-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_validate.c#L37-L281)

## Overview
Validates a BRIN (Block Range Index) operator class by performing comprehensive checks on its operator family, ensuring all required support functions and operators are present with correct signatures.

## Definition

```c
bool
brinvalidate(Oid opclassoid)
```
## Detailed Description
The  function serves as the validation routine for BRIN operator classes, implementing the  interface for the BRIN access method. It performs extensive validation to ensure that a BRIN operator class is properly constructed and contains all necessary components.

The validation process includes:

1. **Support Function Validation**: Checks that all required support functions (OPCINFO, ADDVALUE, CONSISTENT, UNION, and optional OPTIONS) are present with correct signatures
2. **Operator Validation**: Verifies that operators have valid strategy numbers (1-63), proper signatures returning boolean, and are configured for search purposes only
3. **Completeness Checks**: Ensures that operator/function groups are complete for all data type combinations within the operator family
4. **Cross-type Support**: Handles validation of cross-type operators and functions, allowing for families that may not require complete cross-type support

The function validates the entire operator family associated with the given operator class, which means some checks are redundant when validating multiple operator classes within the same family, but this duplication is accepted to keep the validation API simple.

## Parameters / Member Variables
- : The OID of the BRIN operator class to validate

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - SearchSysCacheList1
  - [check_amproc_signature](../c/check_amproc_signature.md)
  - [check_amoptsproc_signature](../c/check_amoptsproc_signature.md)
  - [check_amop_signature](../c/check_amop_signature.md)
  - [identify_opfamily_groups](../i/identify_opfamily_groups.md)
  - [format_procedure](../f/format_procedure.md)
  - [format_operator](../f/format_operator.md)
  - [format_type_be](../f/format_type_be.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [brinhandler](brinhandler.md)

## Notes and Other Information
- Returns  if the operator class passes all validation checks,  otherwise
- Issues INFO-level error reports for each validation failure encountered, allowing multiple issues to be reported in a single validation run
- Validates support function signatures for mandatory functions:
  - BRIN_PROCNUM_OPCINFO: 
  - BRIN_PROCNUM_ADDVALUE: 
  - BRIN_PROCNUM_CONSISTENT: 
  - BRIN_PROCNUM_UNION: 
  - BRIN_PROCNUM_OPTIONS: Uses  for validation
- BRIN does not support ORDER BY operators, so any operators with sort family specifications are rejected
- The function handles optional support functions (numbers beyond the mandatory range) without signature checking
- Cross-type operator groups without any support functions are allowed to pass validation, accommodating families that don't require complete cross-type support

## Simplified Source

```c
bool brinvalidate(Oid opclassoid) {
    bool result = true;
    HeapTuple classtup, familytup;
    Form_pg_opclass classform;
    Form_pg_opfamily familyform;
    Oid opfamilyoid, opcintype;
    char *opclassname, *opfamilyname;
    CatCList *proclist, *oprlist;
    uint64 allfuncs = 0, allops = 0;
    List *grouplist;
    OpFamilyOpFuncGroup *opclassgroup;
    int i;
    ListCell *lc;

    // Fetch opclass and opfamily information
    classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    if (!HeapTupleIsValid(classtup))
        elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
    classform = (Form_pg_opclass) GETSTRUCT(classtup);

    opfamilyoid = classform->opcfamily;
    opcintype = classform->opcintype;
    opclassname = NameStr(classform->opcname);

    familytup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyoid));
    if (!HeapTupleIsValid(familytup))
        elog(ERROR, "cache lookup failed for operator family %u", opfamilyoid);
    familyform = (Form_pg_opfamily) GETSTRUCT(familytup);
    opfamilyname = NameStr(familyform->opfname);

    // Get all operators and support functions
    oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
    proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

    // Validate support functions
    for (i = 0; i < proclist->n_members; i++) {
        HeapTuple proctup = &proclist->members[i]->tuple;
        Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
        bool ok;

        // Check required function signatures
        switch (procform->amprocnum) {
            case BRIN_PROCNUM_OPCINFO:
                ok = check_amproc_signature(procform->amproc, INTERNALOID, true,
                                          1, 1, INTERNALOID);
                break;
            case BRIN_PROCNUM_ADDVALUE:
                ok = check_amproc_signature(procform->amproc, BOOLOID, true,
                                          4, 4, INTERNALOID, INTERNALOID,
                                          INTERNALOID, INTERNALOID);
                break;
            case BRIN_PROCNUM_CONSISTENT:
                ok = check_amproc_signature(procform->amproc, BOOLOID, true,
                                          3, 4, INTERNALOID, INTERNALOID,
                                          INTERNALOID, INT4OID);
                break;
            case BRIN_PROCNUM_UNION:
                ok = check_amproc_signature(procform->amproc, BOOLOID, true,
                                          3, 3, INTERNALOID, INTERNALOID,
                                          INTERNALOID);
                break;
            case BRIN_PROCNUM_OPTIONS:
                ok = check_amoptsproc_signature(procform->amproc);
                break;
            default:
                // Allow optional functions but validate range
                if (procform->amprocnum < BRIN_FIRST_OPTIONAL_PROCNUM ||
                    procform->amprocnum > BRIN_LAST_OPTIONAL_PROCNUM) {
                    ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                  errmsg("invalid support number %d",
                                        procform->amprocnum)));
                    result = false;
                    continue;
                }
                ok = true;
                break;
        }

        if (!ok) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("function %s has wrong signature",
                                format_procedure(procform->amproc))));
            result = false;
        }

        allfuncs |= ((uint64) 1) << procform->amprocnum;
    }

    // Validate operators
    for (i = 0; i < oprlist->n_members; i++) {
        HeapTuple oprtup = &oprlist->members[i]->tuple;
        Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

        // Check strategy number range
        if (oprform->amopstrategy < 1 || oprform->amopstrategy > 63) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("invalid strategy number %d",
                                oprform->amopstrategy)));
            result = false;
        } else {
            // Track strategy numbers for same-type operators
            if (oprform->amoplefttype == oprform->amoprighttype)
                allops |= ((uint64) 1) << oprform->amopstrategy;
        }

        // BRIN doesn't support ORDER BY operators
        if (oprform->amoppurpose != AMOP_SEARCH ||
            OidIsValid(oprform->amopsortfamily)) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("invalid ORDER BY specification")));
            result = false;
        }

        // Check operator signature
        if (!check_amop_signature(oprform->amopopr, BOOLOID,
                                 oprform->amoplefttype, oprform->amoprighttype)) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator %s has wrong signature",
                                format_operator(oprform->amopopr))));
            result = false;
        }
    }

    // Check completeness for each operator/function group
    grouplist = identify_opfamily_groups(oprlist, proclist);
    opclassgroup = NULL;

    foreach(lc, grouplist) {
        OpFamilyOpFuncGroup *thisgroup = (OpFamilyOpFuncGroup *) lfirst(lc);

        if (thisgroup->lefttype == opcintype && thisgroup->righttype == opcintype)
            opclassgroup = thisgroup;

        // Skip cross-type groups without functions
        if (thisgroup->functionset == 0 && thisgroup->lefttype != thisgroup->righttype)
            continue;

        // Check for complete operator/function sets
        if (thisgroup->operatorset != allops) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("missing operator(s) for types %s and %s",
                                format_type_be(thisgroup->lefttype),
                                format_type_be(thisgroup->righttype))));
            result = false;
        }
        if (thisgroup->functionset != allfuncs) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("missing support function(s) for types %s and %s",
                                format_type_be(thisgroup->lefttype),
                                format_type_be(thisgroup->righttype))));
            result = false;
        }
    }

    // Ensure original opclass is complete
    if (!opclassgroup || opclassgroup->operatorset != allops) {
        ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("operator class \"%s\" is missing operator(s)",
                            opclassname)));
        result = false;
    }

    // Check mandatory support functions
    for (i = 1; i <= BRIN_MANDATORY_NPROCS; i++) {
        if (opclassgroup && (opclassgroup->functionset & (((int64) 1) << i)) != 0)
            continue;
        ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("operator class \"%s\" is missing support function %d",
                            opclassname, i)));
        result = false;
    }

    // Cleanup
    ReleaseCatCacheList(proclist);
    ReleaseCatCacheList(oprlist);
    ReleaseSysCache(familytup);
    ReleaseSysCache(classtup);

    return result;
}
```