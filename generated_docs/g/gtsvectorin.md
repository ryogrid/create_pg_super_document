# gtsvectorin

## Location
src/backend/utils/adt/tsgistidx.c: 89 - 98

## Overview
This function is a stub input function for the gtsvector data type that explicitly prevents input operations and raises an error.

## Definition


## Detailed Description
The gtsvectorin function serves as the input function for the gtsvector data type in PostgreSQL's GiST indexing system. However, it is implemented as an intentional stub that prevents any input operations. When called, it immediately raises a feature not supported error, indicating that gtsvector values cannot be directly input by users. This is by design, as gtsvector is an internal data type used within GiST indexes for tsvector operations and is not intended for direct user manipulation.

## Parameters / Member Variables
- Uses  macro: Standard PostgreSQL function argument structure for SQL-callable functions

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_VOID (macro for returning void from PostgreSQL functions)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md) (for error codes)
  - [errmsg](../e/errmsg.md) (for error messages)
- Called from (representative examples):
  - No direct references found (typically called through PostgreSQL's type system)

## Notes and Other Information
- This function is part of the GiST (Generalized Search Tree) infrastructure for full-text search indexing
- The gtsvector type is for internal use only and should not be directly manipulated by users
- The function follows PostgreSQL's convention of having input/output functions for all data types
- Located in src/backend/utils/adt/tsgistidx.c which contains GiST support functions for tsvector