# CopyToState

## Location
src/include/commands/copy.h: 91 - 127

## Overview
CopyToState is a typedef for CopyToStateData pointer, representing the state structure used throughout PostgreSQL's COPY TO operations for bulk data export.

## Definition


The actual structure being referenced is CopyToStateData:


## Detailed Description
CopyToState encapsulates all state information needed during COPY TO operations, which export data from PostgreSQL tables or queries to external destinations. The structure manages data destination handling (files, programs, frontend connections), character encoding conversion with special consideration for multi-byte encodings, output formatting through function managers, and memory management through separate contexts for copy operations and row processing. A key design consideration is the handling of multi-byte character encodings where ASCII characters might appear as non-first bytes, requiring careful scanning to avoid false matches.

## Parameters / Member Variables
- : Enum specifying the type of copy destination
- : File pointer when copying to a file
- : Message buffer used for all destinations during COPY TO
- : Character encoding of output file or remote side
- : Flag indicating if encoding conversion is required
- : Flag for multi-byte encodings where ASCII can be non-first byte
- : Relation (table) being copied from
- : Query descriptor for executable query to copy from
- : List of attribute numbers to copy
- : Output filename or NULL for STDOUT
- : Flag indicating if filename is a program to execute
- : Callback function for writing data
- : CopyFormatOptions containing formatting parameters
- : WHERE condition for filtering rows (or NULL)
- : Memory context for the entire copy operation
- : Array of function manager info for output functions
- : Memory context for per-row evaluation
- : Counter of total bytes processed

## Dependencies
- Functions called/Symbols referenced:
  - CopyDest (enum for copy destination types)
  - QueryDesc (query execution descriptor)
  - CopyFormatOptions (formatting options structure)
- Called from (representative examples):
  - DoCopy (main COPY command handler)
  - BeginCopyTo (initializes COPY TO operation)
  - DoCopyTo (executes COPY TO operation)
  - EndCopyTo (finalizes COPY TO operation)

## Notes and Other Information
This structure is the central state holder for all COPY TO operations in PostgreSQL. Unlike CopyFromState, it has a simpler design focused on output generation rather than input parsing. The structure includes special handling for multi-byte character encodings, where the encoding_embeds_ascii flag determines whether ASCII scanning needs to use full multi-byte character processing to avoid false matches with trailing bytes. The dual memory context approach (copycontext for operation-wide allocations, rowcontext for per-row processing) enables efficient memory management during large data exports. The structure supports various output destinations including files, programs, and frontend connections through the copy_dest enum and associated callback mechanisms.