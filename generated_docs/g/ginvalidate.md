# ginvalidate

## Location
[src/backend/access/gin/ginvalidate.c:31-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvalidate.c#L31-L276)

## Overview
Validates a GIN (Generalized Inverted Index) operator class to ensure it contains all required support functions and operators with correct signatures and parameters.

## Definition

```c
bool
ginvalidate(Oid opclassoid)
```
## Detailed Description
The  function performs comprehensive validation of a GIN operator class by checking:
1. **Support function validation**: Verifies that all required GIN support functions are present with correct signatures
2. **Operator validation**: Ensures operators have valid strategy numbers (1-63) and proper signatures
3. **Consistency checks**: Validates that operator/function groups are internally consistent
4. **Completeness verification**: Confirms the operator class contains all mandatory functions

The function validates the following GIN support functions:
-  (1): Key comparison function (optional)
-  (2): Extract keys from indexed values (required)
-  (3): Extract keys from query conditions (required)
-  (4): Test whether entry is consistent with query (required if no triconsistent)
-  (5): Compare partial-match query key (optional)
-  (6): Ternary consistency check (required if no consistent)
-  (7): Parse reloptions for index (optional)

## Parameters / Member Variables
- `opclassoid`: OID of the operator class to validate
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
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (src/backend/access/gin/ginutil.c:75)

## Notes and Other Information
- Returns  if the operator class is valid,  if validation errors are found
- Validation errors are reported using  calls
- GIN operator classes must have either GIN_CONSISTENT_PROC or GIN_TRICONSISTENT_PROC (or both)
- GIN does not support ORDER BY operators (amoppurpose must be AMOP_SEARCH)
- Strategy numbers for GIN operators must be between 1 and 63
- All support functions must have matching left/right input types
- The function performs thorough signature checking for each support function type

## Simplified Source

```c
bool
ginvalidate(Oid opclassoid)
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
    int i;

    // Fetch opclass and opfamily information
    classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    classform = (Form_pg_opclass) GETSTRUCT(classtup);

    opfamilyoid = classform->opcfamily;
    opcintype = classform->opcintype;
    opckeytype = classform->opckeytype;
    if (!OidIsValid(opckeytype))
        opckeytype = opcintype;
    opclassname = NameStr(classform->opcname);

    familytup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyoid));
    familyform = (Form_pg_opfamily) GETSTRUCT(familytup);
    opfamilyname = NameStr(familyform->opfname);

    // Get all operators and support functions
    oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
    proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

    // Validate each support function
    for (i = 0; i < proclist->n_members; i++) {
        HeapTuple proctup = &proclist->members[i]->tuple;
        Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
        bool ok = false;

        // Check that left/right types match
        if (procform->amproclefttype != procform->amprocrighttype) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains support function with different left and right input types",
                                opfamilyname)));
            result = false;
        }

        // Only check signatures for our specific opclass
        if (procform->amproclefttype != opcintype)
            continue;

        // Validate function signature based on procedure number
        switch (procform->amprocnum) {
            case GIN_COMPARE_PROC:
                ok = check_amproc_signature(procform->amproc, INT4OID, false,
                                           2, 2, opckeytype, opckeytype);
                break;
            case GIN_EXTRACTVALUE_PROC:
                ok = check_amproc_signature(procform->amproc, INTERNALOID, false,
                                           2, 3, opcintype, INTERNALOID, INTERNALOID);
                break;
            case GIN_EXTRACTQUERY_PROC:
                ok = check_amproc_signature(procform->amproc, INTERNALOID, false,
                                           5, 7, opcintype, INTERNALOID, INT2OID,
                                           INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID);
                break;
            case GIN_CONSISTENT_PROC:
                ok = check_amproc_signature(procform->amproc, BOOLOID, false,
                                           6, 8, INTERNALOID, INT2OID, opcintype, INT4OID,
                                           INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID);
                break;
            case GIN_COMPARE_PARTIAL_PROC:
                ok = check_amproc_signature(procform->amproc, INT4OID, false,
                                           4, 4, opckeytype, opckeytype, INT2OID, INTERNALOID);
                break;
            case GIN_TRICONSISTENT_PROC:
                ok = check_amproc_signature(procform->amproc, CHAROID, false,
                                           7, 7, INTERNALOID, INT2OID, opcintype, INT4OID,
                                           INTERNALOID, INTERNALOID, INTERNALOID);
                break;
            case GIN_OPTIONS_PROC:
                ok = check_amoptsproc_signature(procform->amproc);
                break;
            default:
                ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                              errmsg("invalid support function number %d", procform->amprocnum)));
                result = false;
                continue;
        }

        if (!ok) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("wrong signature for support function %d", procform->amprocnum)));
            result = false;
        }
    }

    // Validate operators
    for (i = 0; i < oprlist->n_members; i++) {
        HeapTuple oprtup = &oprlist->members[i]->tuple;
        Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

        // Check strategy number range
        if (oprform->amopstrategy < 1 || oprform->amopstrategy > 63) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("invalid strategy number %d", oprform->amopstrategy)));
            result = false;
        }

        // GIN doesn't support ORDER BY
        if (oprform->amoppurpose != AMOP_SEARCH || OidIsValid(oprform->amopsortfamily)) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("invalid ORDER BY specification for operator")));
            result = false;
        }

        // Check operator signature
        if (!check_amop_signature(oprform->amopopr, BOOLOID,
                                 oprform->amoplefttype, oprform->amoprighttype)) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator has wrong signature")));
            result = false;
        }
    }

    // Check opclass completeness
    grouplist = identify_opfamily_groups(oprlist, proclist);
    opclassgroup = NULL;

    // Find our specific opclass group
    ListCell *lc;
    foreach(lc, grouplist) {
        OpFamilyOpFuncGroup *thisgroup = (OpFamilyOpFuncGroup *) lfirst(lc);
        if (thisgroup->lefttype == opcintype && thisgroup->righttype == opcintype) {
            opclassgroup = thisgroup;
            break;
        }
    }

    // Verify required functions are present
    for (i = 1; i <= GINNProcs; i++) {
        if (opclassgroup && (opclassgroup->functionset & (((uint64) 1) << i)) != 0)
            continue;  // Function present
        if (i == GIN_COMPARE_PROC || i == GIN_COMPARE_PARTIAL_PROC || i == GIN_OPTIONS_PROC)
            continue;  // Optional function
        if (i == GIN_CONSISTENT_PROC || i == GIN_TRICONSISTENT_PROC)
            continue;  // Need one or the other

        ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("missing support function %d", i)));
        result = false;
    }

    // Must have either CONSISTENT or TRICONSISTENT
    if (!opclassgroup ||
        ((opclassgroup->functionset & (1 << GIN_CONSISTENT_PROC)) == 0 &&
         (opclassgroup->functionset & (1 << GIN_TRICONSISTENT_PROC)) == 0)) {
        ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("missing support function %d or %d",
                            GIN_CONSISTENT_PROC, GIN_TRICONSISTENT_PROC)));
        result = false;
    }

    // Clean up
    ReleaseCatCacheList(proclist);
    ReleaseCatCacheList(oprlist);
    ReleaseSysCache(familytup);
    ReleaseSysCache(classtup);

    return result;
}
```