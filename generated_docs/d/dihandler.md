# dihandler

## Location
src/test/modules/dummy_index_am/dummy_index_am.c: 279 - 329

## Overview
The main handler function for the dummy index access method that returns an IndexAmRoutine structure containing all the callback functions and parameters that define the access method's capabilities and behavior.

## Definition
Datum dihandler(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the entry point and main handler for the dummy index access method. It creates and initializes an IndexAmRoutine structure that defines the complete interface for the access method. The function sets various capability flags (all to false or restrictive values) and assigns callback functions for different index operations.

The dummy index AM is designed as a minimal test implementation that demonstrates the index access method framework without providing real indexing functionality. All capability flags are set to their most restrictive values, and many callback functions are set to NULL, indicating unsupported operations.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function arguments macro (no explicit parameters)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for IndexAmRoutine)
  - [dibuild](dibuild.md)
  - [dibuildempty](dibuildempty.md)  
  - [diinsert](diinsert.md)
  - [dibulkdelete](dibulkdelete.md)
  - [divacuumcleanup](divacuumcleanup.md)
  - [dicostestimate](dicostestimate.md)
  - [dioptions](dioptions.md)
  - [divalidate](divalidate.md)
  - [dibeginscan](dibeginscan.md)
  - [direscan](direscan.md)
  - [diendscan](diendscan.md)
- Data types used:
  - [IndexAmRoutine](../I/IndexAmRoutine.md)
  - Datum
- Constants used:
  - VACUUM_OPTION_NO_PARALLEL
  - InvalidOid

## Notes and Other Information
- This is the main entry point function for the dummy index access method
- Located in src/test/modules/dummy_index_am/dummy_index_am.c:279-329
- Returns a Datum containing a pointer to the IndexAmRoutine structure
- All capability flags (amcanorder, amcanunique, etc.) are set to false, indicating minimal functionality
- Many callback functions are set to NULL, indicating unsupported operations (amgettuple, amgetbitmap, etc.)
- Part of PostgreSQL's extensible index access method framework testing infrastructure
- The function would typically be registered with PostgreSQL's access method catalog to make the dummy AM available for use