# word_matches

## Location
src/bin/psql/tab-complete.c: 1468 - 1473

## Overview
A pattern matching function used in PostgreSQL's psql tab completion system to determine if a word matches a given pattern with support for wildcards, alternatives, and case sensitivity control.

## Definition


## Detailed Description
The `word_matches` function implements a flexible pattern matching algorithm that supports multiple features essential for tab completion. It can match exact strings, wildcard patterns (using '*'), alternative patterns (using '|'), and negated patterns (using '\!' prefix). The function is case-sensitive or case-insensitive based on the `case_sensitive` parameter. It uses a loop to process alternative patterns separated by '|' characters, and for each alternative, it checks for wildcards and performs appropriate string matching using either standard `strncmp` or PostgreSQL's case-insensitive `pg_strncasecmp`.

## Parameters / Member Variables
- `pattern`: The pattern string to match against, supporting wildcards (*), alternatives (|), and negation (\!)
- `word`: The target word to be matched against the pattern
- `case_sensitive`: Boolean flag controlling whether matching is case-sensitive or case-insensitive

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - strncmp (standard C library function)
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (PostgreSQL case-insensitive string comparison)
  - cimatch (internal macro for conditional case-sensitive/insensitive comparison)
- Called from (representative examples):
  - cimatch (recursive call for negated patterns)
  - [TailMatchesImpl](../T/TailMatchesImpl.md) (tab completion matching)
  - [MatchesImpl](../M/MatchesImpl.md) (tab completion matching)
  - [HeadMatchesImpl](../H/HeadMatchesImpl.md) (tab completion matching)

## Notes and Other Information
- NULL patterns match anything (returns true)
- Supports negated patterns using '\!' prefix, implemented via recursive call
- Wildcard '*' can appear anywhere in a pattern alternative and matches any substring
- Multiple alternatives in a pattern are separated by '|' characters
- Uses the cimatch macro internally to switch between case-sensitive and case-insensitive comparison
- Essential component of psql's sophisticated tab completion system for SQL command and object name completion