# birthinfo

## Location
src/interfaces/ecpg/test/expected/preproc-variable.c: 56 - 77

## Overview
A struct definition used in ECPG (Embedded SQL in C) test cases to represent basic personal information including birth year and age.

## Definition


## Detailed Description
The  struct is a simple data structure defined in PostgreSQL's ECPG test suite. It serves as a test case for handling struct definitions in embedded SQL preprocessing. The struct contains two basic fields for storing personal temporal information - a birth year stored as a long integer and an age stored as a short integer.

This struct is part of the ECPG preprocessor test infrastructure, specifically testing how the preprocessor handles C struct definitions that will be used in embedded SQL contexts.

## Parameters / Member Variables
- : A long integer representing the birth year or timestamp
- : A short integer representing the person's age

## Dependencies
- Functions called/Symbols referenced:
  - None (basic struct definition)
- Called from (representative examples):
  - [varchar_1](../v/varchar_1.md) (test variable)
  - [personal_indicator](../p/personal_indicator.md) (test variable)
  - [varchar_6](../v/varchar_6.md) (test variable)

## Notes and Other Information
- This is a test structure used specifically in ECPG preprocessor testing
- Located in the expected output file for ECPG variable preprocessing tests
- The struct definition includes preprocessor line directives (#line) that reference the original .pgc source file
- Used as a template for testing how embedded SQL preprocessor handles struct types