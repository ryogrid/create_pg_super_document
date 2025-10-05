# identify_opfamily_groups

## Location
[src/backend/access/index/amvalidate.c:43-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/amvalidate.c#L43-L151)

## Overview
Groups operators and support functions by datatype combinations within an operator family, creating a structured representation for validation purposes.

## Definition

```c
List *
identify_opfamily_groups(CatCList *oprlist, CatCList *proclist)
```
## Detailed Description
This function analyzes an operator family's operators and support functions to create OpFamilyOpFuncGroup structures. Each group represents a unique lefttype/righttype datatype combination and tracks which operator strategies and support function numbers are present using bitmasks. The function processes ordered catalog lists concurrently, ensuring all operators and functions for each datatype pair are grouped together. Strategy numbers and function numbers are stored as bits in uint64 fields, supporting up to 63 different strategies/functions per group.

## Parameters / Member Variables
- `oprlist`: CatCList of operators (pg_amop entries) for the operator family, must be ordered
- `proclist`: CatCList of support functions (pg_amproc entries) for the operator family, must be ordered

## Dependencies
- Functions called/Symbols referenced:
  - [CatCList](../C/CatCList.md) (catalog cache list structure)
  - [OpFamilyOpFuncGroup](../O/OpFamilyOpFuncGroup.md) (result structure type)
  - Form_pg_amop (operator tuple form)
  - Form_pg_amproc (support function tuple form)
  - GETSTRUCT (macro to extract tuple structure)
  - [palloc](../p/palloc.md) (memory allocation)
  - [lappend](../l/lappend.md) (list append function)
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md)
  - [ginvalidate](../g/ginvalidate.md)
  - [gistvalidate](../g/gistvalidate.md)
  - [hashvalidate](../h/hashvalidate.md)
  - [btvalidate](../b/btvalidate.md)
  - [spgvalidate](../s/spgvalidate.md)

## Notes and Other Information
- Requires ordered catalog lists to function correctly; will error if lists are unordered
- Uses concurrent advancement through both lists to maintain efficiency
- Supports operator strategies and function numbers 1-63 (bit positions in uint64)
- Critical component of access method validation infrastructure
- Located in src/backend/access/index/amvalidate.c:43-151

## Simplified Source

```c
List *identify_opfamily_groups(CatCList *oprlist, CatCList *proclist) {
    List *result = NIL;
    OpFamilyOpFuncGroup *thisgroup = NULL;
    Form_pg_amop oprform = NULL;
    Form_pg_amproc procform = NULL;
    int io = 0, ip = 0;

    // Verify that input lists are ordered for correct processing
    if (!oprlist->ordered || !proclist->ordered)
        elog(ERROR, "cannot validate operator family without ordered data");

    // Initialize iterators for both lists
    if (io < oprlist->n_members) {
        oprform = (Form_pg_amop) GETSTRUCT(&oprlist->members[io]->tuple);
        io++;
    }
    if (ip < proclist->n_members) {
        procform = (Form_pg_amproc) GETSTRUCT(&proclist->members[ip]->tuple);
        ip++;
    }

    // Process all operators and functions concurrently
    while (oprform || procform) {
        // Add operator to current group if it matches
        if (oprform && thisgroup &&
            oprform->amoplefttype == thisgroup->lefttype &&
            oprform->amoprighttype == thisgroup->righttype) {

            if (oprform->amopstrategy > 0 && oprform->amopstrategy < 64)
                thisgroup->operatorset |= ((uint64) 1) << oprform->amopstrategy;

            // Advance to next operator
            oprform = (io < oprlist->n_members) ?
                     (Form_pg_amop) GETSTRUCT(&oprlist->members[io++]->tuple) : NULL;
            continue;
        }

        // Add function to current group if it matches
        if (procform && thisgroup &&
            procform->amproclefttype == thisgroup->lefttype &&
            procform->amprocrighttype == thisgroup->righttype) {

            if (procform->amprocnum > 0 && procform->amprocnum < 64)
                thisgroup->functionset |= ((uint64) 1) << procform->amprocnum;

            // Advance to next function
            procform = (ip < proclist->n_members) ?
                      (Form_pg_amproc) GETSTRUCT(&proclist->members[ip++]->tuple) : NULL;
            continue;
        }

        // Create new group for next datatype combination
        thisgroup = (OpFamilyOpFuncGroup *) palloc(sizeof(OpFamilyOpFuncGroup));

        // Determine datatypes from whichever comes first
        if (oprform && (!procform ||
            (oprform->amoplefttype < procform->amproclefttype ||
             (oprform->amoplefttype == procform->amproclefttype &&
              oprform->amoprighttype < procform->amprocrighttype)))) {
            thisgroup->lefttype = oprform->amoplefttype;
            thisgroup->righttype = oprform->amoprighttype;
        } else {
            thisgroup->lefttype = procform->amproclefttype;
            thisgroup->righttype = procform->amprocrighttype;
        }

        thisgroup->operatorset = thisgroup->functionset = 0;
        result = lappend(result, thisgroup);
    }

    return result;
}
```