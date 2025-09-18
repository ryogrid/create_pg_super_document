# get_encoding_id

## Location
src/bin/initdb/initdb.c: 843 - 859

## Overview
This function validates an encoding name and returns its corresponding numeric encoding ID, terminating the program if the encoding is invalid.

## Definition


## Detailed Description
The  function validates a server encoding name and returns its corresponding integer ID. It first checks if the encoding name is non-null and non-empty, then uses  to validate the encoding and retrieve its ID. If the encoding name is invalid, null, or empty, the function calls  to terminate the program with an error message. This function is critical for ensuring that only valid encodings are used during database initialization.

## Parameters / Member Variables
- : A string containing the name of the encoding to validate and convert to ID (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_valid_server_encoding](../p/pg_valid_server_encoding.md): PostgreSQL function to validate server encoding and return ID
  - [pg_fatal](../p/pg_fatal.md): PostgreSQL function to log fatal error and terminate program
- Called from (representative examples):
  - AUTHTRUST_WARNING: Used in authentication warning message generation
  - [setup_locale_encoding](../s/setup_locale_encoding.md): Used during locale and encoding setup

## Notes and Other Information
- This function never returns an invalid encoding ID - it either returns a valid ID or terminates the program
- Handles NULL and empty string inputs gracefully by providing descriptive error messages
- The error message distinguishes between NULL input (shown as "(null)") and the actual encoding name
- Critical for database initialization to ensure encoding consistency
- Uses  which means this function will not return if the encoding is invalid
- Part of the encoding validation infrastructure in initdb