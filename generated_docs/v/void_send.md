# void_send

## Location
src/backend/utils/adt/pseudotypes.c: 285 - 302

## Overview
The void_send function is an output function for the void pseudo-type that handles serializing void values to binary format during PostgreSQL's binary protocol communication.

## Definition
Datum void_send(PG_FUNCTION_ARGS)

## Detailed Description
The void_send function is responsible for serializing void values to binary format in PostgreSQL's binary protocol. The function creates an empty string buffer using PostgreSQL's string buffer infrastructure, then returns it as a bytea (binary array) type. This ensures that void values are consistently represented as empty binary data during client-server communication using the binary protocol.

## Parameters / Member Variables
- Uses PostgreSQL's standard function argument macro PG_FUNCTION_ARGS which provides access to function call context
- buf: StringInfoData buffer used to construct the binary output

## Dependencies
- Functions called/Symbols referenced:
  - pq_begintypsend (initializes binary output buffer)
  - pq_endtypsend (finalizes binary output buffer and returns bytea)
  - PG_RETURN_BYTEA_P (macro for returning bytea datum)
- Called from (representative examples):
  - (No direct references found in codebase)

## Notes and Other Information
- Part of PostgreSQL's pseudo-type system located in src/backend/utils/adt/pseudotypes.c
- Complements void_recv by providing the output serialization counterpart
- Always produces empty binary data to maintain consistency with void type semantics
- Uses PostgreSQL's standard binary serialization infrastructure (pq_begintypsend/pq_endtypsend)
- The empty string representation ensures minimal bandwidth usage for void values in binary protocol