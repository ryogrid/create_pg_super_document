# spgvalidate

## Location
[src/backend/access/spgist/spgvalidate.c:39-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvalidate.c#L39-L331)

## Overview
The `spgvalidate` function serves as the validation function for SP-GiST (Space-Partitioned Generalized Search Tree) operator classes, ensuring that all required support functions and operators are properly defined with correct signatures.

## Definition
```c
bool spgvalidate(Oid opclassoid)
```

## Detailed Description
The `spgvalidate` function performs comprehensive validation of an SP-GiST operator class. It checks that all required support functions are present and have the correct signatures, validates that all operators have appropriate strategy numbers and signatures, and ensures consistency between operators and functions within the operator family.

The function performs several key validation steps:
1. **Support Function Validation**: Checks that all required SP-GiST support functions (config, choose, picksplit, inner_consistent, leaf_consistent, and optionally compress and options) are present with correct signatures
2. **Operator Validation**: Ensures operators have valid strategy numbers (1-63) and correct signatures
3. **Cross-Reference Validation**: Verifies consistency between operators and support functions within the same operator family
4. **Configuration Validation**: Calls the config function and validates its output parameters

Some validation checks are performed across the entire operator family and may be redundant when validating multiple operator classes in the same family, but the performance impact is minimal.

## Parameters / Member Variables
- `opclassoid`: The OID of the SP-GiST operator class to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - SearchSysCacheList1
  - [identify_opfamily_groups](../i/identify_opfamily_groups.md)
  - [check_amproc_signature](../c/check_amproc_signature.md)
  - [check_amoptsproc_signature](../c/check_amoptsproc_signature.md)
  - [check_amop_signature](../c/check_amop_signature.md)
  - OidFunctionCall2
  - [get_op_rettype](../g/get_op_rettype.md)
  - [opfamily_can_sort_type](../o/opfamily_can_sort_type.md)
  - [format_procedure](../f/format_procedure.md)
  - [format_operator](../f/format_operator.md)
  - [format_type_be](../f/format_type_be.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [spghandler](spghandler.md) (in spgutils.c:82)

## Notes and Other Information
- Returns `true` if the operator class is valid, `false` if validation errors are found
- Validation errors are reported using `ereport(INFO, ...)` calls rather than throwing errors
- The function handles both search operators and ORDER BY operators for SP-GiST indexes
- Special handling for compress functions: when leaf and attribute types are the same, the compress function is optional
- The validation covers support function numbers 1-7 corresponding to: config, choose, picksplit, inner_consistent, leaf_consistent, compress, and options procedures
- Strategy numbers for operators must be between 1 and 63
- Cross-type support functions are not used in SP-GiST, so validation only checks same-type function groups

## Simplified Source

```c
bool
spgvalidate(Oid opclassoid)
{
    bool result = true;
    HeapTuple classtup, familytup;
    Form_pg_opclass classform;
    Form_pg_opfamily familyform;
    Oid opfamilyoid, opcintype, opckeytype;
    char *opclassname, *opfamilyname;
    CatCList *proclist, *oprlist;
    List *grouplist;
    OpFamilyOpFuncGroup *opclassgroup;
    spgConfigIn configIn;
    spgConfigOut configOut;
    Oid configOutLeafType = InvalidOid;
    ListCell *lc;
    int i;

    // Fetch operator class information
    classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    if (!HeapTupleIsValid(classtup))
        elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
    classform = (Form_pg_opclass) GETSTRUCT(classtup);

    opfamilyoid = classform->opcfamily;
    opcintype = classform->opcintype;
    opckeytype = classform->opckeytype;
    opclassname = NameStr(classform->opcname);

    // Fetch operator family information
    familytup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyoid));
    if (!HeapTupleIsValid(familytup))
        elog(ERROR, "cache lookup failed for operator family %u", opfamilyoid);
    familyform = (Form_pg_opfamily) GETSTRUCT(familytup);
    opfamilyname = NameStr(familyform->opfname);

    // Get all operators and support functions
    oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
    proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));
    grouplist = identify_opfamily_groups(oprlist, proclist);

    // Validate each support function
    for (i = 0; i < proclist->n_members; i++) {
        HeapTuple proctup = &proclist->members[i]->tuple;
        Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
        bool ok;

        // Check that left/right types match (required for SP-GiST)
        if (procform->amproclefttype != procform->amprocrighttype) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains support function with different left and right input types",
                                opfamilyname)));
            result = false;
        }

        // Validate function signature based on support function number
        switch (procform->amprocnum) {
            case SPGIST_CONFIG_PROC:
                ok = check_amproc_signature(procform->amproc, VOIDOID, true, 2, 2, INTERNALOID, INTERNALOID);

                // Test the config function
                configIn.attType = procform->amproclefttype;
                memset(&configOut, 0, sizeof(configOut));
                OidFunctionCall2(procform->amproc, PointerGetDatum(&configIn), PointerGetDatum(&configOut));

                // Determine leaf type and validate consistency
                configOutLeafType = OidIsValid(opckeytype) ? opckeytype : procform->amproclefttype;
                if (OidIsValid(configOut.leafType) && configOutLeafType != configOut.leafType) {
                    ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                  errmsg("SP-GiST leaf data type %s does not match declared type %s",
                                        format_type_be(configOut.leafType), format_type_be(configOutLeafType))));
                    result = false;
                    configOutLeafType = configOut.leafType;
                }
                break;

            case SPGIST_CHOOSE_PROC:
            case SPGIST_PICKSPLIT_PROC:
            case SPGIST_INNER_CONSISTENT_PROC:
                ok = check_amproc_signature(procform->amproc, VOIDOID, true, 2, 2, INTERNALOID, INTERNALOID);
                break;

            case SPGIST_LEAF_CONSISTENT_PROC:
                ok = check_amproc_signature(procform->amproc, BOOLOID, true, 2, 2, INTERNALOID, INTERNALOID);
                break;

            case SPGIST_COMPRESS_PROC:
                ok = check_amproc_signature(procform->amproc, configOutLeafType, true, 1, 1, procform->amproclefttype);
                break;

            case SPGIST_OPTIONS_PROC:
                ok = check_amoptsproc_signature(procform->amproc);
                break;

            default:
                ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                              errmsg("operator family \"%s\" contains function with invalid support number %d",
                                    opfamilyname, procform->amprocnum)));
                result = false;
                continue;
        }

        if (!ok) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains function with wrong signature for support number %d",
                                opfamilyname, procform->amprocnum)));
            result = false;
        }
    }

    // Validate operators
    for (i = 0; i < oprlist->n_members; i++) {
        HeapTuple oprtup = &oprlist->members[i]->tuple;
        Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);
        Oid op_rettype;

        // Check strategy number range
        if (oprform->amopstrategy < 1 || oprform->amopstrategy > 63) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains operator with invalid strategy number %d",
                                opfamilyname, oprform->amopstrategy)));
            result = false;
        }

        // Validate ORDER BY operators
        if (oprform->amoppurpose != AMOP_SEARCH) {
            op_rettype = get_op_rettype(oprform->amopopr);
            if (!opfamily_can_sort_type(oprform->amopsortfamily, op_rettype)) {
                ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                              errmsg("operator family \"%s\" contains invalid ORDER BY specification",
                                    opfamilyname)));
                result = false;
            }
        } else {
            op_rettype = BOOLOID;
        }

        // Check operator signature
        if (!check_amop_signature(oprform->amopopr, op_rettype, oprform->amoplefttype, oprform->amoprighttype)) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains operator with wrong signature",
                                opfamilyname)));
            result = false;
        }
    }

    // Check for missing operators/functions in each group
    opclassgroup = NULL;
    foreach(lc, grouplist) {
        OpFamilyOpFuncGroup *thisgroup = (OpFamilyOpFuncGroup *) lfirst(lc);

        if (thisgroup->lefttype == opcintype && thisgroup->righttype == opcintype)
            opclassgroup = thisgroup;

        // Check for missing operators
        if (thisgroup->operatorset == 0) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" is missing operators for types %s and %s",
                                opfamilyname, format_type_be(thisgroup->lefttype), format_type_be(thisgroup->righttype))));
            result = false;
        }

        // Check for missing support functions (only for same-type groups)
        if (thisgroup->lefttype == thisgroup->righttype) {
            for (int j = 1; j <= SPGISTNProc; j++) {
                if ((thisgroup->functionset & (((uint64) 1) << j)) == 0 && j != SPGIST_OPTIONS_PROC) {
                    ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                  errmsg("operator family \"%s\" is missing support function %d for type %s",
                                        opfamilyname, j, format_type_be(thisgroup->lefttype))));
                    result = false;
                }
            }
        }
    }

    // Ensure the target opclass is properly supported
    if (!opclassgroup) {
        ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("operator class \"%s\" is missing operators", opclassname)));
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