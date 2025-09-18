# pgresParamDesc

## Location
src/interfaces/libpq/libpq-int.h: 113 - 115

## Overview
A structure that holds metadata about a single parameter of a prepared statement in PostgreSQL's libpq client library.

## Definition


## Detailed Description
The  structure is used internally by libpq to store information about parameters in prepared statements. It contains the PostgreSQL Object Identifier (OID) of the parameter's data type, which allows the client library to understand what type of data is expected for each parameter when executing a prepared statement.

This structure is typically used in arrays to describe all parameters of a prepared statement, with each element corresponding to one parameter in the statement's parameter list.

## Parameters / Member Variables
- : The PostgreSQL Object Identifier (OID) representing the data type of this parameter

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL Object Identifier type)
- Called from (representative examples):
  - Used in  structure as  field
  - Allocated and used in  during prepared statement processing

## Notes and Other Information
- This structure is part of libpq's internal implementation and is not directly exposed to client applications
- The structure is allocated as an array when processing prepared statement descriptions from the server
- Memory allocation uses  and is initialized with  to zero out the structure
- The typedef creates the alias  which is commonly used throughout the codebase