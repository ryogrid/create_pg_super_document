# rtrim1

## Location
src/backend/utils/adt/oracle_compat.c: 766 - 796

## Overview
A simplified right-trim function that removes trailing whitespace characters (spaces only) from the right side of a text string.

## Definition


## Detailed Description
The rtrim1 function provides a streamlined version of PostgreSQL's rtrim functionality with a fixed character set of just space characters (' '). It serves as a wrapper around the internal dotrim function, specifically configured to trim only trailing spaces from text input. This function is part of PostgreSQL's Oracle compatibility layer, providing behavior similar to Oracle's RTRIM function when called without specifying a trim set.

## Parameters / Member Variables
- : The input text string from which trailing spaces will be removed

## Dependencies
- Functions called/Symbols referenced:
  - dotrim
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is located in src/backend/utils/adt/oracle_compat.c:766-796
- This is a PostgreSQL built-in function designed for Oracle compatibility
- Unlike the generic rtrim function, this version has a hardcoded trim set of single space character
- Uses the internal dotrim function with parameters (string_data, string_length, " ", 1, false, true)
- The last two boolean parameters to dotrim indicate: left_trim=false, right_trim=true