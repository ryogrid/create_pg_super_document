# debugtup

## Location
[src/backend/access/common/printtup.c:462-488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L462-L488)

## Overview
The debugtup function prints debug information for a single tuple to standard output, designed for interactive backend debugging sessions where developers need to examine tuple contents.

## Definition


## Detailed Description
The debugtup function serves as a debugging utility that outputs the contents of a PostgreSQL tuple in a human-readable format. It iterates through all attributes in the tuple descriptor, extracts each non-null attribute value, converts it to its string representation using the appropriate output function, and displays it using the printatt function.

This function is primarily used during development and debugging phases to inspect tuple data flowing through the query execution pipeline. It provides a convenient way to examine the structure and values of tuples without requiring complex debugging tools.

The function processes each attribute by:
1. Retrieving the attribute value from the slot
2. Skipping null attributes 
3. Obtaining the appropriate output function for the attribute's data type
4. Converting the attribute value to its string representation
5. Displaying the formatted attribute information via printatt
6. Adding a separator line after processing all attributes

## Parameters / Member Variables
- : A TupleTableSlot containing the tuple data to be debugged, including the tuple descriptor and attribute values
- : A DestReceiver pointer (unused in this function but required by the DestReceiver interface)

## Dependencies
- Functions called/Symbols referenced:
  - slot_getattr: Extracts attribute values from the tuple slot
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md): Retrieves the output function OID for a given data type
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md): Calls the appropriate output function to convert attribute values to strings
  - [printatt](../p/printatt.md): Formats and displays attribute information to standard output
  - TupleDescAttr: Macro to access tuple descriptor attribute information
- Called from (representative examples):
  - [print_slot](../p/print_slot.md): Uses debugtup for printing slot contents in debugging contexts
  - MJ_debugtup: Merge join debugging macro that utilizes debugtup functionality

## Notes and Other Information
- The function always returns true, indicating successful processing
- Null attributes are silently skipped during output
- The function adds a "----" separator line after displaying all attributes
- This is part of the DestReceiver interface pattern used throughout PostgreSQL's result handling system
- Located in src/backend/access/common/printtup.c:462-488
- Primarily intended for development and debugging purposes, not for production query result formatting