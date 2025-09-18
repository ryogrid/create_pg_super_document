# tsvectorsend

## Location
src/backend/utils/adt/tsvector.c: 407 - 445

## Overview
The  function converts a TSVector data type to binary format for efficient network transmission and storage in PostgreSQL's binary protocol.

## Definition


## Detailed Description
This function is the binary send function for the TSVector data type, responsible for serializing TSVector data into PostgreSQL's binary wire format. The binary format is structured and compact, beginning with the number of lexemes as a 32-bit integer, followed by each lexeme's text (null-terminated), the number of positions as a 16-bit integer, and finally the position data as 16-bit WordEntryPos values.

The function uses PostgreSQL's pq_send* family of functions to construct a binary representation that can be efficiently transmitted over the network or stored in binary format. Each lexeme is sent with its exact length and null-terminated, followed by position count and the raw position data containing both position and weight information encoded in WordEntryPos format.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing the TSVector input parameter

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extract TSVector from function arguments
  - ARRPTR: Get pointer to word entries array
  - STRPTR: Get pointer to string data
  - POSDATAPTR: Get pointer to position data
  - POSDATALEN: Get length of position data
  - pq_begintypsend: Initialize binary send buffer
  - pq_sendint32: Send 32-bit integer in binary format
  - pq_sendint16: Send 16-bit integer in binary format
  - pq_sendtext: Send text data in binary format
  - pq_sendbyte: Send single byte
  - pq_endtypsend: Finalize binary send buffer
  - PG_RETURN_BYTEA_P: Return binary data result
- Called from (representative examples):
  - PostgreSQL binary protocol handlers
  - Client-server communication for TSVector data transfer

## Notes and Other Information
- The binary format is platform-independent and handles byte order conversion
- Lexeme strings are explicitly null-terminated in the binary format since they are not null-terminated in the internal TSVector structure
- Position data is sent as raw WordEntryPos values, preserving both position and weight information
- The format is designed for efficient parsing by the corresponding  function
- This function is part of PostgreSQL's type system binary I/O infrastructure