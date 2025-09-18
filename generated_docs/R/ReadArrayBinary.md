# ReadArrayBinary

## Location
src/backend/utils/adt/arrayfuncs.c: 1454 - 1547

## Overview
Reads and deserializes individual array elements from a binary data buffer, converting them using element-specific receive procedures while tracking nulls and calculating storage requirements.

## Definition


## Detailed Description
ReadArrayBinary is a static helper function that handles the low-level deserialization of array elements from binary format. It processes each element by reading its length prefix, handling NULL values (indicated by -1 length), and using element-specific receive procedures to convert binary data to internal Datum format. The function efficiently manages memory by using read-only StringInfo structures to avoid data copying, and performs comprehensive validation including buffer bounds checking and proper consumption verification.

The function also calculates the total storage space required for all elements, including alignment padding, and detects potential memory allocation overflows. For variable-length elements, it ensures data is not toasted and properly accounts for storage requirements using PostgreSQL's attribute alignment functions.

## Parameters / Member Variables
- : StringInfo buffer containing the binary array data
- : Number of array elements to read
- : Function pointer to the element type's receive procedure  
- : Type-specific parameter for the receive procedure
- : Type modifier for elements
- : Length of element type (-1 for variable length)
- : Whether elements are passed by value or reference
- : Alignment requirement for element type
- : Output array to store converted Datum values
- : Output array to store null indicators
- : Output flag indicating presence of any null elements
- : Output total size needed for data storage with alignment

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint
  - ReceiveFunctionCall
  - initReadOnlyStringInfo
  - PG_DETOAST_DATUM
  - att_addlength_datum
  - att_align_nominal
  - AllocSizeIsValid
  - MaxAllocSize
- Called from (representative examples):
  - array_recv

## Notes and Other Information
The function uses -1 as a special length value to indicate NULL elements in the binary format. It employs read-only StringInfo structures to avoid unnecessary data copying during element processing. Comprehensive validation ensures that receive procedures consume exactly the expected amount of data. For variable-length types, the function automatically detoasts values to ensure proper storage calculations. Memory overflow protection prevents creation of arrays exceeding MaxAllocSize limits, maintaining system stability during large array operations.