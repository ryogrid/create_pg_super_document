# heap_getsysattr

## Location
src/backend/access/common/heaptuple.c: 723 - 775

## Overview
heap_getsysattr fetches the value of a system attribute for a heap tuple, providing access to PostgreSQL's built-in tuple metadata such as transaction IDs, command IDs, table OID, and tuple identifier.

## Definition


## Detailed Description
heap_getsysattr is a specialized function that extracts system attribute values from heap tuples. System attributes are PostgreSQL's built-in columns that provide metadata about each tuple, including transaction visibility information and tuple location data. Unlike regular user-defined attributes, system attributes are never stored explicitly in the tuple data but are derived from the tuple header and structure.

The function handles six different system attributes:
- ctid (SelfItemPointerAttributeNumber): The tuple's physical location (page and item number)
- xmin (MinTransactionIdAttributeNumber): Transaction ID that inserted this tuple
- xmax (MaxTransactionIdAttributeNumber): Transaction ID that deleted/updated this tuple
- cmin/cmax (MinCommandIdAttributeNumber/MaxCommandIdAttributeNumber): Command IDs within the transaction
- tableoid (TableOidAttributeNumber): OID of the table containing this tuple

The function is designed as a support routine for heap_getattr() and is only called after it has been determined that the requested attribute number refers to a system attribute.

## Parameters / Member Variables
- : HeapTuple containing the tuple data and metadata
- : System attribute number (negative values identifying specific system attributes)
- : TupleDesc structure (may be unused for system attributes but included for interface consistency)
- : Output parameter set to indicate if the attribute is null (always false for system attributes)

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (convert pointer to Datum for ctid)
  - TransactionIdGetDatum (convert transaction IDs to Datum)
  - CommandIdGetDatum (convert command IDs to Datum)  
  - ObjectIdGetDatum (convert OID to Datum)
  - HeapTupleHeaderGetRawXmin, HeapTupleHeaderGetRawXmax, HeapTupleHeaderGetRawCommandId (extract raw values from tuple header)
  - System attribute number constants (SelfItemPointerAttributeNumber, etc.)
  - elog (error logging)
- Called from (representative examples):
  - heap_getattr
  - tts_heap_getsysattr
  - tts_buffer_heap_getsysattr
  - expanded_record_fetch_field
  - HeapTupleClearHeapOnly

## Notes and Other Information
- System attributes are never null in PostgreSQL - the function always sets *isnull to false
- The cmin and cmax attributes are actually aliases for the same field in the tuple header, which can be a combo command ID
- System attribute numbers are negative, distinguishing them from regular user attributes (which are positive)
- Critical for implementing SQL access to system columns like ctid, xmin, xmax, cmin, cmax, and tableoid
- Used by the query executor to provide system attribute values in SELECT statements
- Essential for PostgreSQL's MVCC (Multi-Version Concurrency Control) system visibility
- The function provides the bridge between PostgreSQL's internal tuple representation and the SQL-level system column interface
- Part of the core tuple access API used throughout PostgreSQL's query processing system
- The tableoid attribute is particularly important for inheritance and partitioning features