# gistvalidate

## Location
[src/backend/access/gist/gistvalidate.c:33-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistvalidate.c#L33-L289)

## Overview
Validates the completeness and correctness of a GiST (Generalized Search Tree) operator class by checking its support functions and operators against GiST access method requirements.

## Definition

```c
bool
gistvalidate(Oid opclassoid)
```
## Detailed Description
The  function performs comprehensive validation of a GiST operator class to ensure it conforms to the requirements of the GiST access method. It validates both the structure and signatures of support functions and operators within the operator class and its associated operator family.

The validation process includes:
1. **Support Function Validation**: Checks that all GiST support functions have correct signatures and are registered with matching left/right input types
2. **Operator Validation**: Verifies that operators have valid strategy numbers, correct signatures, and proper ORDER BY support when applicable
3. **Completeness Check**: Ensures the operator class contains all required support functions
4. **Cross-Reference Validation**: Validates relationships between operators and their corresponding support functions

For each support function (GIST_CONSISTENT_PROC through GIST_SORTSUPPORT_PROC), the function validates the expected signature using  or . It also ensures that ORDER BY operators have corresponding distance functions and that the operator result types are compatible with the specified btree operator families.

## Parameters / Member Variables
- : The OID of the GiST operator class to validate

## Dependencies
- Functions called/Symbols referenced:
  -  - Cache lookups for operator class and family information
  -  - Retrieve operators and support functions
  -  - Validate support function signatures
  -  - Validate options support function signature
  -  - Validate operator signatures
  -  - [Group](../G/Group.md) operators and functions by datatype combinations
  -  - Verify btree compatibility for ORDER BY operators
  -  - Look up distance procedures for ORDER BY operators
  -  - Get operator return type
- Called from:
  -  at src/backend/access/gist/gist.c:97

## Notes and Other Information
- The function returns  if the operator class is valid,  otherwise
- Issues are reported using  rather than throwing errors, allowing multiple validation issues to be reported in a single validation run  
- Required GiST support functions include: CONSISTENT, UNION, PENALTY, PICKSPLIT, and EQUAL procedures
- Optional functions include: COMPRESS, DECOMPRESS, DISTANCE, FETCH, OPTIONS, and SORTSUPPORT procedures
- All GiST support functions must have matching left and right input types
- ORDER BY operators require corresponding distance functions and compatible btree operator families
- The validation leverages the processed symbols , , , , and  for comprehensive signature and compatibility checking

## Simplified Source

```c
bool gistvalidate(Oid opclassoid) {
    bool result = true;

    // Get operator class and family information
    HeapTuple classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    if (!HeapTupleIsValid(classtup))
        elog(ERROR, "cache lookup failed for operator class %u", opclassoid);

    Form_pg_opclass classform = (Form_pg_opclass) GETSTRUCT(classtup);
    Oid opfamilyoid = classform->opcfamily;
    Oid opcintype = classform->opcintype;
    Oid opckeytype = OidIsValid(classform->opckeytype) ? classform->opckeytype : opcintype;

    // Get all operators and support functions
    CatCList *oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
    CatCList *proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

    // Validate each support function
    for (int i = 0; i < proclist->n_members; i++) {
        Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(&proclist->members[i]->tuple);

        // Check matching left/right types
        if (procform->amproclefttype != procform->amprocrighttype) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("support function has different left and right input types")));
            result = false;
        }

        // Validate function signatures based on procedure number
        bool ok = true;
        switch (procform->amprocnum) {
            case GIST_CONSISTENT_PROC:
                ok = check_amproc_signature(procform->amproc, BOOLOID, false, 5, 5, ...);
                break;
            case GIST_UNION_PROC:
                ok = check_amproc_signature(procform->amproc, opckeytype, false, 2, 2, ...);
                break;
            // ... other procedure types
            default:
                result = false;
                continue;
        }

        if (!ok) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("function has wrong signature")));
            result = false;
        }
    }

    // Validate operators
    for (int i = 0; i < oprlist->n_members; i++) {
        Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(&oprlist->members[i]->tuple);

        // Validate strategy numbers and signatures
        if (oprform->amopstrategy < 1) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator has invalid strategy number")));
            result = false;
        }

        // Additional ORDER BY operator validation...
    }

    // Check operator class completeness
    List *grouplist = identify_opfamily_groups(oprlist, proclist);
    OpFamilyOpFuncGroup *opclassgroup = find_opclass_group(grouplist, opcintype);

    for (int i = 1; i <= GISTNProcs; i++) {
        if (is_required_proc(i) && !has_proc(opclassgroup, i)) {
            ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                          errmsg("operator class missing support function %d", i)));
            result = false;
        }
    }

    // Cleanup
    ReleaseCatCacheList(proclist);
    ReleaseCatCacheList(oprlist);
    ReleaseSysCache(classtup);

    return result;
}
```