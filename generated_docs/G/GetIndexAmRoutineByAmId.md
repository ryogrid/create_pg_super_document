# GetIndexAmRoutineByAmId

## Location
[src/backend/access/index/amapi.c:56-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/amapi.c#L56-L113)

## Overview
GetIndexAmRoutineByAmId looks up an index access method handler by its OID and retrieves the IndexAmRoutine struct containing the method's function pointers and capabilities.

## Definition
IndexAmRoutine *GetIndexAmRoutineByAmId(Oid amoid, bool noerror)

## Detailed Description
This function serves as a lookup mechanism for PostgreSQL's index access method infrastructure. It takes an access method OID and performs several validation steps:

1. Searches the pg_am system catalog for the specified access method OID
2. Validates that the access method is specifically an index access method (AMTYPE_INDEX)
3. Ensures the access method has a valid handler function
4. Calls the handler function via GetIndexAmRoutine to obtain the IndexAmRoutine struct

The function includes comprehensive error handling, with the ability to return NULL instead of throwing errors when the noerror parameter is true. This makes it suitable for both critical code paths and exploratory lookups.

## Parameters / Member Variables
- : The OID of the index access method to look up
- : If true, returns NULL on errors instead of throwing exceptions; if false, throws appropriate error messages

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [GetIndexAmRoutine](GetIndexAmRoutine.md)
  - Form_pg_am
  - regproc
  - AMTYPE_INDEX
  - RegProcedureIsValid
- Called from (representative examples):
  - [amvalidate](../a/amvalidate.md)
  - [ConstructTupleDescriptor](../C/ConstructTupleDescriptor.md)
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamily](../A/AlterOpFamily.md)
  - [IndexSupportsBackwardScan](../I/IndexSupportsBackwardScan.md)
  - [indexam_property](../i/indexam_property.md)

## Notes and Other Information
- Located in src/backend/access/index/amapi.c:56-113
- This function is a critical component of PostgreSQL's pluggable index access method architecture
- The returned IndexAmRoutine must be freed by the caller using pfree()
- Validates that the access method is specifically for indexes, not other types of access methods like table access methods
- Part of the public API for accessing index access method functionality programmatically

## Simplified Source

```c
IndexAmRoutine *
GetIndexAmRoutineByAmId(Oid amoid, bool noerror)
{
    HeapTuple tuple;
    Form_pg_am amform;
    regproc amhandler;

    // Look up access method in system catalog
    tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
    if (!HeapTupleIsValid(tuple))
    {
        if (noerror)
            return NULL;
        elog(ERROR, "cache lookup failed for access method %u", amoid);
    }
    amform = (Form_pg_am) GETSTRUCT(tuple);

    // Verify it's an index access method
    if (amform->amtype != AMTYPE_INDEX)
    {
        if (noerror)
        {
            ReleaseSysCache(tuple);
            return NULL;
        }
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("access method \"%s\" is not of type %s",
                               NameStr(amform->amname), "INDEX")));
    }

    amhandler = amform->amhandler;

    // Verify handler function exists
    if (!RegProcedureIsValid(amhandler))
    {
        if (noerror)
        {
            ReleaseSysCache(tuple);
            return NULL;
        }
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("index access method \"%s\" does not have a handler",
                               NameStr(amform->amname))));
    }

    ReleaseSysCache(tuple);

    // Call handler function to get API struct
    return GetIndexAmRoutine(amhandler);
}
```