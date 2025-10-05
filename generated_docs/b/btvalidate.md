# btvalidate

## Location
[src/backend/access/nbtree/nbtvalidate.c:41-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtvalidate.c#L41-L292)

## Overview
Validates a btree operator class by checking the consistency and completeness of its operators and support functions within the broader operator family.

## Definition

```c
bool
btvalidate(Oid opclassoid)
```
## Detailed Description
The  function performs comprehensive validation of a btree operator class to ensure it contains all required operators and support functions with correct signatures. It validates both the individual components and the overall consistency of the operator family. The function checks:

1. **Support Function Validation**: Verifies that each support function has the correct signature based on its procedure number (BTORDER_PROC, BTSORTSUPPORT_PROC, BTINRANGE_PROC, BTEQUALIMAGE_PROC, BTOPTIONS_PROC).

2. **Operator Validation**: Ensures all operators have valid strategy numbers (1-5), proper signatures returning boolean, and are configured for search purposes only (no ORDER BY support).

3. **Completeness Checks**: Verifies that the operator family contains complete sets of operators (all 5 comparison operators: <, <=, =, >=, >) and required support functions for each supported data type combination.

4. **Cross-type Operator Coverage**: Ensures the operator family provides operators for all possible combinations of supported data types to maximize query optimization opportunities.

The validation covers the entire operator family, not just the specific operator class, which may result in some redundant checks when validating multiple classes in the same family.

## Parameters / Member Variables
- `opclassoid`: The OID of the btree operator class to validate
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
  - [list_append_unique_oid](../l/list_append_unique_oid.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from:
  - [bthandler](bthandler.md) (in btree access method handler)

## Notes and Other Information
- The function accepts some redundancy in validation when multiple operator classes exist in the same family, prioritizing simplicity over performance
- Optional support functions (sortsupport, in_range, equalimage) are not required for validation to pass
- The function reports validation errors as INFO messages rather than throwing errors, allowing multiple issues to be reported in a single validation run
- Cross-type operator completeness is enforced to ensure optimal query planning capabilities
- Special handling exists for in_range functions that may have RHS types not otherwise relevant to the opfamily (e.g., datetime with interval offsets)

## Simplified Source

```c
bool btvalidate(Oid opclassoid)
{
    bool result = true;
    HeapTuple classtup, familytup;
    Form_pg_opclass classform;
    Form_pg_opfamily familyform;
    Oid opfamilyoid, opcintype;
    char *opclassname, *opfamilyname;
    CatCList *proclist, *oprlist;
    List *grouplist, *familytypes;
    OpFamilyOpFuncGroup *opclassgroup;
    int usefulgroups, i;
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
    for (i = 0; i < proclist->n_members; i++)
    {
        HeapTuple proctup = &proclist->members[i]->tuple;
        Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
        bool ok;

        // Check function signatures based on procedure number
        switch (procform->amprocnum)
        {
            case BTORDER_PROC:
                ok = check_amproc_signature(procform->amproc, INT4OID, true,
                                          2, 2, procform->amproclefttype,
                                          procform->amprocrighttype);
                break;
            case BTSORTSUPPORT_PROC:
                ok = check_amproc_signature(procform->amproc, VOIDOID, true,
                                          1, 1, INTERNALOID);
                break;
            case BTINRANGE_PROC:
                ok = check_amproc_signature(procform->amproc, BOOLOID, true,
                                          5, 5, procform->amproclefttype,
                                          procform->amproclefttype,
                                          procform->amprocrighttype,
                                          BOOLOID, BOOLOID);
                break;
            case BTEQUALIMAGE_PROC:
                ok = check_amproc_signature(procform->amproc, BOOLOID, true,
                                          1, 1, OIDOID);
                break;
            case BTOPTIONS_PROC:
                ok = check_amoptsproc_signature(procform->amproc);
                break;
            default:
                ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                              errmsg("operator family \"%s\" contains function with invalid support number %d",
                                     opfamilyname, procform->amprocnum)));
                result = false;
                continue;
        }

        if (!ok)
        {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains function with wrong signature for support number %d",
                                 opfamilyname, procform->amprocnum)));
            result = false;
        }
    }

    // Validate operators
    for (i = 0; i < oprlist->n_members; i++)
    {
        HeapTuple oprtup = &oprlist->members[i]->tuple;
        Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

        // Check strategy numbers (1-5 for btree)
        if (oprform->amopstrategy < 1 || oprform->amopstrategy > BTMaxStrategyNumber)
        {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains operator with invalid strategy number %d",
                                 opfamilyname, oprform->amopstrategy)));
            result = false;
        }

        // Check that only search operators are supported (no ORDER BY)
        if (oprform->amoppurpose != AMOP_SEARCH || OidIsValid(oprform->amopsortfamily))
        {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains invalid ORDER BY specification",
                                 opfamilyname)));
            result = false;
        }

        // Check operator signature (should return boolean)
        if (!check_amop_signature(oprform->amopopr, BOOLOID,
                                 oprform->amoplefttype, oprform->amoprighttype))
        {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" contains operator with wrong signature",
                                 opfamilyname)));
            result = false;
        }
    }

    // Check completeness of operator/function groups
    grouplist = identify_opfamily_groups(oprlist, proclist);
    usefulgroups = 0;
    opclassgroup = NULL;
    familytypes = NIL;

    foreach(lc, grouplist)
    {
        OpFamilyOpFuncGroup *thisgroup = (OpFamilyOpFuncGroup *) lfirst(lc);

        // Skip groups with only in_range functions
        if (thisgroup->operatorset == 0 &&
            thisgroup->functionset == (1 << BTINRANGE_PROC))
            continue;

        usefulgroups++;

        // Track the exact opclass group and all family types
        if (thisgroup->lefttype == opcintype && thisgroup->righttype == opcintype)
            opclassgroup = thisgroup;

        familytypes = list_append_unique_oid(familytypes, thisgroup->lefttype);
        familytypes = list_append_unique_oid(familytypes, thisgroup->righttype);

        // Check for complete operator set (all 5 comparison operators)
        if (thisgroup->operatorset != ((1 << BTLessStrategyNumber) |
                                      (1 << BTLessEqualStrategyNumber) |
                                      (1 << BTEqualStrategyNumber) |
                                      (1 << BTGreaterEqualStrategyNumber) |
                                      (1 << BTGreaterStrategyNumber)))
        {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" is missing operators for types %s and %s",
                                 opfamilyname, format_type_be(thisgroup->lefttype),
                                 format_type_be(thisgroup->righttype))));
            result = false;
        }

        // Check for required comparison function
        if ((thisgroup->functionset & (1 << BTORDER_PROC)) == 0)
        {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator family \"%s\" is missing support function for types %s and %s",
                                 opfamilyname, format_type_be(thisgroup->lefttype),
                                 format_type_be(thisgroup->righttype))));
            result = false;
        }
    }

    // Check that the opclass itself is properly supported
    if (!opclassgroup)
    {
        ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("operator class \"%s\" is missing operators", opclassname)));
        result = false;
    }

    // Check for complete cross-type operator coverage
    if (usefulgroups != (list_length(familytypes) * list_length(familytypes)))
    {
        ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("operator family \"%s\" is missing cross-type operators", opfamilyname)));
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