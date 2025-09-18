# ForeignDataWrapper

## Location
[src/include/foreign/foreign.h:24-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/foreign/foreign.h#L24-L32)

## Overview
ForeignDataWrapper is a structure that represents a foreign data wrapper in PostgreSQL, which defines the interface for accessing external data sources through the Foreign Data Wrapper (FDW) system.

## Definition


## Detailed Description
The ForeignDataWrapper structure encapsulates all the metadata needed to define a foreign data wrapper in PostgreSQL. It serves as the foundational component of the FDW system, storing essential information about the wrapper including its identity, ownership, associated handler and validator functions, and configuration options. This structure is used throughout the PostgreSQL codebase when working with foreign data sources, enabling the system to properly route operations to the appropriate external data handlers.

## Parameters / Member Variables
- : The unique object identifier (OID) for this foreign data wrapper
- : The OID of the user who owns this FDW
- : The string name of the foreign data wrapper
- : The OID of the handler function that implements the FDW interface, or 0 if none
- : The OID of the validator function for checking FDW options, or 0 if none
- : A list of DefElem structures containing FDW-specific options

## Dependencies
- Functions called/Symbols referenced:
  - Oid (built-in type)
  - [List](../L/List.md) (PostgreSQL list structure)
  - [DefElem](../D/DefElem.md) (option definition element)
- Called from (representative examples):
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md)
  - [GetForeignDataWrapperExtended](../G/GetForeignDataWrapperExtended.md)
  - [CreateForeignServer](../C/CreateForeignServer.md)
  - [AlterForeignServer](../A/AlterForeignServer.md)
  - [CreateUserMapping](../C/CreateUserMapping.md)
  - [CreateForeignTable](../C/CreateForeignTable.md)

## Notes and Other Information
- This structure is defined in src/include/foreign/foreign.h and is fundamental to PostgreSQL's FDW architecture
- The handler function is responsible for implementing the actual FDW interface callbacks
- The validator function is used to validate FDW and server options during DDL operations
- Options are stored as a list of DefElem structures, allowing flexible configuration
- Used extensively in foreign data wrapper management commands and operations