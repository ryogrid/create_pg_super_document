# gtsquery_penalty

## Location
src/backend/utils/adt/tsquery_gist.c: 139 - 154

## Overview
A GiST penalty function for TSQuery indexes that calculates the cost of inserting a new TSQuery signature into an existing index node by measuring the Hamming distance between signatures.

## Definition


## Detailed Description
The gtsquery_penalty function implements the penalty method for GiST (Generalized Search Tree) indexes on TSQuery data types. It calculates the penalty (cost) of inserting a new TSQuery signature into an existing index node by computing the Hamming distance between the original signature and the new signature.

This function is a key component of the GiST framework for TSQuery indexes, used during index insertion to determine the optimal subtree for placing new entries. Lower penalty values indicate better placement choices, helping maintain index efficiency.

The function extracts TSQuery signatures from the GiST entry parameters, uses the hemdist function to calculate the Hamming distance between them, and returns this distance as the penalty value.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro for function arguments:
  - Argument 0: GISTENTRY pointer containing the original TSQuery signature
  - Argument 1: GISTENTRY pointer containing the new TSQuery signature to be inserted
  - Argument 2: Pointer to float where the calculated penalty will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetTSQuerySign](../D/DatumGetTSQuerySign.md) (macro to extract TSQuery signature from Datum)
  - [GISTENTRY](../G/GISTENTRY.md) (GiST entry structure)
  - TSQuerySign (TSQuery signature type)
  - [hemdist](../h/hemdist.md) (function to calculate Hamming distance between signatures)
  - PG_GETARG_POINTER (macro to get function arguments)
  - PG_RETURN_POINTER (macro to return pointer value)
- Called from:
  - No direct references found (likely called through GiST function table)

## Notes and Other Information
- This is a PostgreSQL function following the fmgr interface convention
- Part of the GiST access method implementation for TSQuery data types
- The penalty calculation directly uses Hamming distance, making signature similarity the primary factor for insertion decisions
- Located in src/backend/utils/adt/tsquery_gist.c:139-154
- Returns a Datum containing a pointer to the calculated penalty float value