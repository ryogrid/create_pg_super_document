# ECPG_COMPAT_ORACLE

## Location
src/interfaces/ecpg/preproc/preproc_extern.h: 129 - 132

## Overview
ECPG_COMPAT_ORACLE is an enumeration constant that represents Oracle compatibility mode in the PostgreSQL ECPG (Embedded SQL in C) preprocessor.

## Definition


## Detailed Description
ECPG_COMPAT_ORACLE is a member of the COMPAT_MODE enumeration that enables Oracle compatibility mode in the ECPG preprocessor. When this mode is active, the ECPG preprocessor modifies its behavior to be more compatible with Oracle's embedded SQL syntax and semantics. This allows developers to port Oracle-based embedded SQL applications to PostgreSQL with minimal code changes.

The symbol is defined in the ecpglib_extern.h header file and is used throughout the ECPG system to conditionally enable Oracle-specific behavior patterns. It works in conjunction with the ORACLE_MODE macro which checks if the current compatibility mode is set to ECPG_COMPAT_ORACLE.

## Parameters / Member Variables
- Part of  enumeration
- Represents Oracle compatibility mode setting
- Used as a flag to enable Oracle-specific SQL processing behavior

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration constant)
- Called from (representative examples):
  -  function in src/interfaces/ecpg/preproc/ecpg.c:191
  - Referenced by  macro in src/interfaces/ecpg/ecpglib/ecpglib_extern.h:28
  - Referenced in src/interfaces/ecpg/preproc/preproc_extern.h:134

## Notes and Other Information
- This enumeration value is set when the ECPG preprocessor is invoked with the "-C ORACLE" command-line option
- The ORACLE_MODE(X) macro is used throughout the codebase to check if Oracle compatibility mode is enabled: 
- Oracle compatibility mode affects SQL parsing, data type handling, and other embedded SQL behaviors to match Oracle's expectations
- This is part of PostgreSQL's broader strategy to support migration from other database systems by providing compatibility modes
- Located in src/interfaces/ecpg/ecpglib/ecpglib_extern.h at lines 22-26