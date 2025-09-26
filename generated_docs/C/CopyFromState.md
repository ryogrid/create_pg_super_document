# CopyFromState

## Location
[src/include/commands/copy.h:90-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/copy.h#L90-L90)

## Overview
CopyFromState is a typedef for CopyFromStateData pointer, representing the comprehensive state structure used throughout PostgreSQL's COPY FROM operations for bulk data import.

## Definition


The actual structure being referenced is CopyFromStateData:


## Detailed Description
CopyFromState encapsulates all state information needed during COPY FROM operations, which import data from external sources into PostgreSQL tables. The structure manages multiple aspects including data source handling (files, programs, frontend connections), character encoding conversion, input buffering and parsing, attribute processing with type conversion, error handling, and performance optimization through buffer reuse. The design supports various input formats (text, CSV, binary) and provides comprehensive error reporting with context information.

## Parameters / Member Variables
- : Enum specifying the type of copy source (file, frontend, etc.)
- : File pointer when copying from a file
- : Message buffer for frontend communication
- : End-of-line type detected in input
- : Character encoding of input file or remote side
- : Flag indicating if encoding conversion is required
- : OID of encoding conversion function
- : Relation (table) being copied into
- : List of attribute numbers to copy
- : Input filename or NULL for STDIN
- : Flag indicating if filename is a program to execute
- : Callback function for reading data
- : CopyFormatOptions containing formatting parameters
- : Per-column selective conversion flags
- : WHERE condition for filtering rows
- : Current relation name for error messages
- : Current line number for error reporting
- : Current attribute name for error context
- : Current attribute value for error context
- : Flag to suppress detailed error context
- : Memory context for copy operation
- : Count of attributes with default values
- : Array of input conversion functions
- : Array of type parameters for input functions
- : Error save context for soft error handling
- : Total count of soft errors encountered
- : Mapping of default attribute numbers
- : Array of default value expressions
- : Flags indicating DEFAULT markers found
- : Flag for volatile default expressions
- : Range table entry list for query processing
- : Permission information list
- : Compiled WHERE clause expression
- : State for transition table capture
- : Buffer for processed attribute data
- : Maximum number of fields
- : Array of pointers to raw field data
- : Buffer containing current input line
- : Flag indicating line buffer validity
- : Buffer for encoding-converted input data
- : Current position in input buffer
- : Length of data in input buffer
- : EOF flag for input buffer
- : Error flag for input buffer
- : Buffer for raw input data
- : Current position in raw buffer
- : Length of data in raw buffer
- : EOF flag for raw buffer
- : Total bytes processed counter

## Dependencies
- Functions called/Symbols referenced:
  - CopySource (enum for copy source types)
  - EolType (enum for end-of-line types)
  - CopyFormatOptions (formatting options structure)
  - ErrorSaveContext (soft error handling)
  - TransitionCaptureState (transition table support)
- Called from (representative examples):
  - DoCopy (main COPY command handler)
  - BeginCopyFrom (initializes COPY FROM operation)
  - CopyFrom (executes COPY FROM operation)
  - EndCopyFrom (finalizes COPY FROM operation)

## Notes and Other Information
This structure is the central state holder for all COPY FROM operations in PostgreSQL. It uses a sophisticated buffering system with separate raw and converted input buffers to optimize performance during large data imports. The structure supports soft error handling, allowing operations to continue and collect multiple errors rather than stopping at the first error. The dual buffer system (raw_buf for unconverted data, input_buf for database-encoding data) enables efficient character set conversion. Buffer sizes are defined as constants (INPUT_BUF_SIZE and RAW_BUF_SIZE = 65536 bytes each) with convenience macros for calculating available bytes.