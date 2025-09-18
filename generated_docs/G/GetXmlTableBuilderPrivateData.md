# GetXmlTableBuilderPrivateData

## Location
src/backend/utils/adt/xml.c: 4658 - 4683

## Overview
A static inline function that safely retrieves and validates private data from a TableFuncScanState for XML table processing operations.

## Definition
static inline XmlTableBuilderData *GetXmlTableBuilderPrivateData(TableFuncScanState *state, const char *fname)

## Detailed Description
This function provides a type-safe mechanism to access the private XmlTableBuilderData stored within a TableFuncScanState structure. It performs validation checks to ensure the state parameter is valid and that the private data contains the expected magic number (XMLTABLE_CONTEXT_MAGIC) to prevent corruption or misuse. The function serves as a defensive programming practice to ensure data integrity when working with XML table functions in PostgreSQL's executor.

The function is designed to be called by various XML table processing functions that need to access their shared context data stored in the executor state.

## Parameters / Member Variables
- state: TableFuncScanState* - The executor state containing the private data
- fname: const char* - The name of the calling function (used for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (PostgreSQL type checking macro)
  - elog (PostgreSQL error logging function)
  - XmlTableBuilderData (structure type)
  - XMLTABLE_CONTEXT_MAGIC (validation magic number)
- Called from (representative examples):
  - XmlTableSetDocument
  - XmlTableSetNamespace
  - XmlTableSetRowFilter
  - XmlTableSetColumnFilter
  - XmlTableFetchRow
  - XmlTableGetValue
  - XmlTableDestroyOpaque

## Notes and Other Information
- Marked as static inline for performance optimization since it's called frequently
- Provides defensive validation with magic number checking
- Located in src/backend/utils/adt/xml.c:4658-4683
- Essential safety mechanism for XML table function implementation
- Uses PostgreSQL's elog mechanism for error reporting with function name context