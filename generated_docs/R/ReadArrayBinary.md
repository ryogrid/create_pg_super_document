# ReadArrayBinary

## Location
[src/backend/utils/adt/arrayfuncs.c:1454-1547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1454-L1547)

## Overview
Reads and deserializes individual array elements from a binary data buffer, converting them using element-specific receive procedures while tracking nulls and calculating storage requirements.

## Definition

```c
static void
ReadArrayBinary(StringInfo buf,
				int nitems,
				FmgrInfo *receiveproc,
				Oid typioparam,
				int32 typmod,
				int typlen,
				bool typbyval,
				char typalign,
				Datum *values,
				bool *nulls,
				bool *hasnulls,
				int32 *nbytes)
```
## Detailed Description
ReadArrayBinary is a static helper function that handles the low-level deserialization of array elements from binary format. It processes each element by reading its length prefix, handling NULL values (indicated by -1 length), and using element-specific receive procedures to convert binary data to internal Datum format. The function efficiently manages memory by using read-only StringInfo structures to avoid data copying, and performs comprehensive validation including buffer bounds checking and proper consumption verification.

The function also calculates the total storage space required for all elements, including alignment padding, and detects potential memory allocation overflows. For variable-length elements, it ensures data is not toasted and properly accounts for storage requirements using PostgreSQL's attribute alignment functions.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the binary array data
- `nitems`: Number of array elements to read
- `*receiveproc`: Function pointer to the element type's receive procedure
- `typioparam`: Type-specific parameter for the receive procedure
- `typmod`: Type modifier for elements
- `typlen`: Length of element type (-1 for variable length)
- `typbyval`: Whether elements are passed by value or reference
- `typalign`: Alignment requirement for element type
- `*values`: Output array to store converted Datum values
- `*nulls`: Output array to store null indicators
- `*hasnulls`: Output flag indicating presence of any null elements
- `*nbytes`: Output total size needed for data storage with alignment
## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [ReceiveFunctionCall](ReceiveFunctionCall.md)
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md)
  - PG_DETOAST_DATUM
  - att_addlength_datum
  - att_align_nominal
  - AllocSizeIsValid
  - MaxAllocSize
- Called from (representative examples):
  - [array_recv](../a/array_recv.md)

## Notes and Other Information
The function uses -1 as a special length value to indicate NULL elements in the binary format. It employs read-only StringInfo structures to avoid unnecessary data copying during element processing. Comprehensive validation ensures that receive procedures consume exactly the expected amount of data. For variable-length types, the function automatically detoasts values to ensure proper storage calculations. Memory overflow protection prevents creation of arrays exceeding MaxAllocSize limits, maintaining system stability during large array operations.