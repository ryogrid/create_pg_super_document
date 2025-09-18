# ri_GenerateQual

## Location
[src/backend/utils/adt/ri_triggers.c:1910-1938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1910-L1938)

## Overview
A utility function that generates a WHERE clause equating two variables with proper operator qualification, type casting, and schema qualification for use in dynamically constructed SQL queries.

## Definition


## Detailed Description
This function constructs a WHERE clause fragment that compares two operands using a specified operator. It appends the comparison clause to a StringInfo buffer in the format " sep leftop op rightop". The function ensures that the parser will select the correct operator by adding necessary type casts and schema qualifications. This is particularly important in PostgreSQL's referential integrity system where precise operator selection is crucial for correct constraint checking.

The function delegates the actual operator clause generation to , which handles the complex logic of operator resolution, type casting, and schema qualification.

## Parameters / Member Variables
- : StringInfo buffer to which the WHERE clause fragment will be appended
- : Separator string (typically "AND" or "OR") to be added before the comparison clause
- : String representation of the left operand (should be parenthesized if not a simple variable or parameter)
- : OID of the data type of the left operand
- : OID of the operator to be used for the comparison
- : String representation of the right operand (should be parenthesized if not a simple variable or parameter)
- : OID of the data type of the right operand

## Dependencies
- Functions called/Symbols referenced:
  - : Appends the separator to the buffer
  - : Generates the actual operator clause with proper type casting and qualification

- Called from (representative examples):
  - : Used in primary key matching operations for referential integrity
  - : Used in foreign key restriction checks
  - : Used in foreign key cascade delete operations
  - : Used in foreign key cascade update operations
  - : Used in referential integrity set operations
  - : Used in initial referential integrity constraint checks
  - : Used in partition removal integrity checks

## Notes and Other Information
- This is a static function within the ri_triggers.c file, used exclusively for referential integrity operations
- The function is designed to work with dynamically constructed SQL queries where precise operator selection is critical
- The caller is responsible for ensuring that complex operands (expressions) are properly parenthesized
- The function handles the complexity of PostgreSQL's type system by ensuring proper operator resolution through the  function
- Essential for building WHERE clauses in referential integrity triggers that must work correctly across different data types and operator families