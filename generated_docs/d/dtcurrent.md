# dtcurrent

## Location
src/interfaces/ecpg/compatlib/informix.c: 612 - 617

## Overview
The dtcurrent function obtains the current timestamp and stores it in the provided timestamp variable. It serves as a compatibility wrapper for Informix ESQL/C applications.

## Definition


## Detailed Description
This function is part of PostgreSQL's ECPG (Embedded SQL in C) compatibility library for Informix. It provides a simple interface to get the current date and time as a timestamp. The function acts as a thin wrapper around the PostgreSQL PGTYPES library's timestamp functionality, specifically calling PGTYPEStimestamp_current() to perform the actual timestamp retrieval.

The function internally uses the system's current date and time, converts it to a struct tm format, and then transforms it into PostgreSQL's timestamp format. If any error occurs during the conversion process, the timestamp value may be undefined.

## Parameters / Member Variables
- : Pointer to a timestamp variable where the current timestamp will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPEStimestamp_current](../P/PGTYPEStimestamp_current.md)
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- This function is part of the Informix compatibility layer in ECPG
- Located in src/interfaces/ecpg/compatlib/informix.c:612-617
- Provides compatibility for applications migrating from Informix to PostgreSQL
- The function does not perform error checking on the input parameter
- The timestamp format follows PostgreSQL's internal timestamp representation