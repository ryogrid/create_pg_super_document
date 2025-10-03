# hashvalidate

## Location
[src/backend/access/hash/hashvalidate.c:47-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashvalidate.c#L47-L274)

## Overview
The hashvalidate function is a validator for hash operator classes that checks the consistency and completeness of hash access method operator families, ensuring all required operators and support functions are properly defined and have correct signatures.

## Definition
```c
bool hashvalidate(Oid opclassoid)
```

## Detailed Description
The hashvalidate function performs comprehensive validation of a hash operator class by examining its associated operator family. It validates that:

1. **Support Functions**: All hash functions (HASHSTANDARD_PROC, HASHEXTENDED_PROC) have matching left/right types and correct signatures, and options functions (HASHOPTIONS_PROC) have proper signatures.

2. **Operators**: All operators have valid strategy numbers (1 to HTMaxStrategyNumber), proper boolean return signatures, and no unsupported ORDER BY specifications.

3. **Completeness**: The operator family contains hash functions for all data types that have operators, and all possible combinations of supported data types have corresponding operators.

4. **Cross-type Support**: Built-in hash operator families should have complete cross-type operator coverage.

The function reports validation errors as INFO messages and returns false if any issues are found, making it useful for system integrity checks and debugging operator family definitions.

## Parameters / Member Variables
- `opclassoid`: The OID of the hash operator class to validate

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - SearchSysCacheList1
  - [check_hash_func_signature](../c/check_hash_func_signature.md)
  - [check_amoptsproc_signature](../c/check_amoptsproc_signature.md)
  - [check_amop_signature](../c/check_amop_signature.md)
  - [identify_opfamily_groups](../i/identify_opfamily_groups.md)
  - [list_append_unique_oid](../l/list_append_unique_oid.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [format_procedure](../f/format_procedure.md)
  - [format_operator](../f/format_operator.md)
  - [format_type_be](../f/format_type_be.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (in hash access method interface)

## Simplified Source
```c
bool hashvalidate(Oid opclassoid) {
    bool result = true;
    List *hashabletypes = NIL;

    // Fetch opclass and opfamily information
    HeapTuple classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    Form_pg_opclass classform = (Form_pg_opclass) GETSTRUCT(classtup);
    Oid opfamilyoid = classform->opcfamily;

    HeapTuple familytup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyoid));
    Form_pg_opfamily familyform = (Form_pg_opfamily) GETSTRUCT(familytup);

    // Get all operators and support functions
    CatCList *oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
    CatCList *proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

    // Validate support functions
    for (int i = 0; i < proclist->n_members; i++) {
        Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(&proclist->members[i]->tuple);

        // Check left/right types match
        if (procform->amproclefttype != procform->amprocrighttype) {
            result = false;
            continue;
        }

        // Validate function signatures based on procedure number
        switch (procform->amprocnum) {
            case HASHSTANDARD_PROC:
            case HASHEXTENDED_PROC:
                if (check_hash_func_signature(procform->amproc, procform->amprocnum,
                                             procform->amproclefttype)) {
                    hashabletypes = list_append_unique_oid(hashabletypes,
                                                          procform->amproclefttype);
                } else {
                    result = false;
                }
                break;
            default:
                result = false;
                break;
        }
    }

    // Validate operators
    for (int i = 0; i < oprlist->n_members; i++) {
        Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(&oprlist->members[i]->tuple);

        // Check strategy numbers and signatures
        if (oprform->amopstrategy < 1 || oprform->amopstrategy > HTMaxStrategyNumber ||
            oprform->amoppurpose != AMOP_SEARCH ||
            !check_amop_signature(oprform->amopopr, BOOLOID,
                                 oprform->amoplefttype, oprform->amoprighttype)) {
            result = false;
        }

        // Ensure hash functions exist for operator types
        if (!list_member_oid(hashabletypes, oprform->amoplefttype) ||
            !list_member_oid(hashabletypes, oprform->amoprighttype)) {
            result = false;
        }
    }

    // Check for complete operator/function groups
    List *grouplist = identify_opfamily_groups(oprlist, proclist);
    // ... additional group validation logic ...

    ReleaseCatCacheList(proclist);
    ReleaseCatCacheList(oprlist);
    ReleaseSysCache(familytup);
    ReleaseSysCache(classtup);

    return result;
}
```

## Notes and Other Information
- The validation covers the entire operator family, so some checks are redundant when validating multiple operator classes in the same family, but this duplication is accepted to keep the amvalidate API simple.
- The function expects hash operator families to be complete with all cross-type operators for built-in types.
- [Hash](../H/Hash.md) access method only supports equality operators (strategy number 1) and does not support ORDER BY operations.
- Located in src/backend/access/hash/hashvalidate.c:47-274.