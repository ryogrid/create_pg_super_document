# serialize_prepare_info

## Location
src/backend/commands/explain.c: 5334 - 5386

## Overview
A helper function that prepares function lookup information needed for tuple serialization in different output formats (text or binary).

## Definition
static void serialize_prepare_info(SerializeDestReceiver *receiver, TupleDesc typeinfo, int nattrs)

## Detailed Description
This function sets up the necessary function lookup information for serializing tuples to different output formats. It's a simplified version of printtup_prepare_info() that handles format preparation for tuple serialization. The function allocates an array of FmgrInfo structures and populates each with the appropriate output function based on the format specification. For text format (format 0), it uses type output functions via getTypeOutputInfo, while for binary format (format 1), it uses binary output functions via getTypeBinaryOutputInfo. The function also handles cleanup of any previously allocated function info and validates that the format code is supported.

## Parameters / Member Variables
- receiver: SerializeDestReceiver pointer containing the destination receiver state and format information
- typeinfo: TupleDesc describing the tuple structure and attribute types
- nattrs: Number of attributes in the tuple descriptor

## Dependencies
- Functions called/Symbols referenced:
  - pfree (memory deallocation)
  - palloc0 (zero-initialized memory allocation)
  - TupleDescAttr (tuple descriptor access macro)
  - getTypeOutputInfo (type system function for text output)
  - getTypeBinaryOutputInfo (type system function for binary output)
  - fmgr_info (function manager info initialization)
  - ereport (error reporting)
- Called from (representative examples):
  - serializeAnalyzeReceive

## Notes and Other Information
- This is a static function only accessible within the explain.c file
- Supports both text (format 0) and binary (format 1) wire protocol formats
- Handles memory management by freeing old function info before allocating new
- Part of PostgreSQL's tuple serialization system for EXPLAIN command output
- Includes error handling for unsupported format codes