# rmdyjul

## Location
src/interfaces/ecpg/compatlib/informix.c: 592 - 603

## Overview
Converts month, day, and year components into a Julian date value in the ECPG Informix compatibility library.

## Definition


## Detailed Description
The  function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility layer. It takes an array of short integers representing month, day, and year components and converts them into a Julian date value. This function serves as a wrapper around the internal  function, converting the short integer inputs to regular integers for PostgreSQL's internal date handling functions.

The function is the inverse operation of , converting from individual date components back to a Julian date representation. It always returns 0, indicating successful conversion.

## Parameters / Member Variables
- : Input array of 3 short integers containing:
  - : Month component
  - : Day component  
  - : Year component
- : Pointer to a date variable where the resulting Julian date will be stored

## Dependencies
- Functions called/Symbols referenced:
  - : Internal PostgreSQL function that performs the actual MDY to Julian date conversion
  - Thu Sep 11 03:47:56 JST 2025: Date type used for the output parameter
- Called from (representative examples):
  - : Used in test functions in the ECPG test suite
  - Referenced in  macro in ecpg_informix.h

## Notes and Other Information
- Located in src/interfaces/ecpg/compatlib/informix.c:592-603
- This function is specifically designed for Informix compatibility in the ECPG interface
- Complements the  function by providing the inverse operation (MDY to Julian vs. Julian to MDY)
- Converts short integers to regular integers to match PostgreSQL's internal function expectations
- Always returns 0 (success) - [error](../e/error.md) handling is presumably done by the underlying  function
- Part of the date conversion utilities that allow Informix applications to work with PostgreSQL's date handling