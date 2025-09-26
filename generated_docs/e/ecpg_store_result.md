# ecpg_store_result

## Location
src/interfaces/ecpg/ecpglib/execute.c: 303 - 455

## Overview
Stores query result data from a PostgreSQL result set into an ECPG variable structure, handling type conversion, memory allocation, and array processing for embedded SQL applications.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's ECPG (Embedded SQL in C) library that transfers data from a PostgreSQL query result set into a client-side variable. This function handles complex scenarios including:

- Array type detection and validation
- Dynamic memory allocation for variable-sized data
- Type-specific data conversion and storage
- Indicator variable management for NULL value handling
- Special handling for character string arrays and varchar types
- Tuple count validation against array size constraints

The function performs comprehensive error checking and uses the ECPG error handling system to report issues such as cardinality violations, type mismatches, and memory allocation failures.

## Parameters / Member Variables
- : Pointer to the PostgreSQL result set containing the query data
- : Zero-based index of the field/column in the result set to process
- : Pointer to the statement structure containing metadata like line number and compatibility mode
- : Pointer to the variable structure that will receive the data, including type information, size constraints, and storage pointers

## Dependencies
- Functions called/Symbols referenced:
  - : Get number of tuples in result set
  - : Determine if field type is an array
  - : Get field type from result set
  - : Raise ECPG errors
  - : Log ECPG messages
  - : Get field format (text/binary)
  - : Get field value as string
  - : Get field value length
  - : Allocate memory with error handling
  - : Convert and store individual field values
  - : Check compatibility mode

- Called from (representative examples):
  - : Descriptor-based data retrieval
  - : Main output processing function

## Notes and Other Information
- Supports both regular variables and arrays with comprehensive size validation
- Implements special handling for char** variables with dynamic memory layout
- Manages both data storage and optional indicator variables for NULL detection
- Uses ECPG-specific error codes and SQL state values for standardized error reporting
- Integrates with INFORMIX compatibility mode for legacy application support
- Memory allocation is handled through ECPG's auto-allocation system with automatic cleanup