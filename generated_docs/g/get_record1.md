# get_record1

## Location
src/interfaces/ecpg/test/expected/preproc-outofscope.c: 230 - 250

## Overview
A static function that fetches a record from a database cursor in ECPG test code, retrieving data into bound variables with proper type and null indicator handling.

## Definition


## Detailed Description
The  function is part of the ECPG test infrastructure that demonstrates cursor record fetching operations. It uses the ECPGdo function to execute a FETCH statement on the previously declared 'mycur' cursor. The function retrieves data into multiple bound variables of different types (integer, character, double) along with their corresponding null indicators.

This function showcases the complex parameter passing required for ECPG cursor fetch operations, demonstrating how embedded SQL FETCH statements are translated into the underlying PostgreSQL client library calls. It's specifically designed to test scenarios where variables are accessed across different scopes in embedded SQL programs.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGdo](../E/ECPGdo.md) (ECPG runtime function for SQL execution)
  - ECPGget_var (ECPG runtime function to retrieve variable addresses)
  - [MYTYPE](../M/MYTYPE.md) (custom data type structure)
  - [MYNULLTYPE](../M/MYNULLTYPE.md) (custom null indicator structure)
  - [mytype](../m/mytype.md) (structure type reference)
  - mynulltype (structure type reference)
  - ECPGt_int, ECPGt_char, ECPGt_double (ECPG type constants)
- Called from (representative examples):
  - [main](../m/main.md) (in the same test file)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Part of the ECPG test suite for validating out-of-scope variable handling
- The function uses complex ECPGdo parameter passing to fetch data into multiple fields with different data types
- Includes automatic error handling via sqlca.sqlcode checking
- The operation corresponds to 'FETCH mycur' in SQL
- Demonstrates proper retrieval of both data fields and their null indicators
- Uses ECPGget_var to dynamically retrieve variable addresses at runtime
- The function expects that the cursor 'mycur' has been previously opened via open_cur1
- Requires variables to have been previously registered and allocated via get_var1
- Each field has both a data target and a null indicator target for complete SQL NULL handling