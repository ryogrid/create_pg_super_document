# BlessTupleDesc

## Location
[src/backend/executor/execTuples.c:2158-2172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2158-L2172)

## Overview
BlessTupleDesc finalizes a tuple descriptor for use with Set-Returning Functions (SRFs) by ensuring that transient RECORD types have valid type information registered in the type cache.

## Definition
TupleDesc BlessTupleDesc(TupleDesc tupdesc)

## Detailed Description
BlessTupleDesc is responsible for making a completed tuple descriptor suitable for use with Set-Returning Functions by ensuring proper type registration. When tuple descriptors are created for transient RECORD datatypes (as opposed to those derived from relcache entries), they lack proper type identification that is required for rowtype Datums returned by functions.

The function checks if the tuple descriptor represents a RECORD type that has not yet been registered (indicated by tdtypeid == RECORDOID and tdtypmod < 0). If this condition is met, it calls assign_record_type_typmod to notify the type cache system of the existence of this type, effectively registering it and assigning a proper typmod. This process is essential for ensuring that dynamically created tuple types can be properly handled by the PostgreSQL type system.

The function is designed as a pass-through operation, returning the same tuple descriptor for notational convenience while potentially modifying its type registration status as a side effect.

## Parameters / Member Variables
- `tupdesc`: A TupleDesc that needs to be finalized for use with SRFs, particularly those representing transient RECORD types

## Dependencies
- Functions called/Symbols referenced:
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md)

- Called from (representative examples):
  - [pg_prepared_xact](../p/pg_prepared_xact.md)
  - [pg_walfile_name_offset](../p/pg_walfile_name_offset.md)
  - [ExecInitExprRec](../E/ExecInitExprRec.md)
  - [ExecEvalWholeRowVar](../E/ExecEvalWholeRowVar.md)
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)
  - [ExecInitFunctionScan](../E/ExecInitFunctionScan.md)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [get_expr_result_type](../g/get_expr_result_type.md)

## Notes and Other Information
- Only affects RECORD-type tuple descriptors that have not been blessed yet (tdtypmod < 0)
- The blessing process is idempotent - calling it multiple times on the same descriptor is safe
- This is a critical step for SRFs returning composite types to ensure proper type system integration
- The function serves as a bridge between dynamically created tuple descriptors and PostgreSQL's type management system
- Returns the input tuple descriptor unchanged except for potential type registration side effects
- Widely used across various PostgreSQL subsystems that deal with composite return types

## Simplified Source

```c
TupleDesc BlessTupleDesc(TupleDesc tupdesc)
{
    // Check if this is an unregistered RECORD type
    if (tupdesc->tdtypeid == RECORDOID && tupdesc->tdtypmod < 0) {
        // Register the type in the type cache system
        assign_record_type_typmod(tupdesc);
    }

    // Return the same descriptor (now potentially blessed)
    return tupdesc;
}
```