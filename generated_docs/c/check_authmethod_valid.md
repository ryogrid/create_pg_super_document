# check_authmethod_valid

## Location
[src/bin/initdb/initdb.c:2560-2574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2560-L2574)

## Overview
Validates that a specified authentication method is supported for a particular connection type during PostgreSQL cluster initialization.

## Definition

```c
static void
check_authmethod_valid(const char *authmethod, const char *const *valid_methods, const char *conntype)
```
## Detailed Description
This function performs validation of authentication methods during initdb execution by:

1. **Method lookup**: Iterates through the array of valid authentication methods for the specified connection type
2. **String comparison**: Uses  to check if the provided authentication method matches any valid option
3. **Early return**: Returns successfully if a match is found
4. **Fatal error**: If no match is found, terminates initdb with a fatal error message indicating the invalid method and connection type

The function ensures that only supported and secure authentication methods are configured during database cluster initialization, preventing misconfigurations that could lead to connection issues or security vulnerabilities.

## Parameters / Member Variables
- : The authentication method string to validate (e.g., "md5", "scram-sha-256", "trust", "peer")
- : Null-terminated array of strings containing valid authentication methods for the connection type
- : Description of the connection type being validated (e.g., "local", "host", "hostssl") for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function for string comparison
  - : PostgreSQL utility function that prints error message and terminates the program

- Called from (representative examples):
  - : Called during initdb to validate authentication methods for different connection types (local, host connections)

## Notes and Other Information
- Function is marked  as it's only used within initdb.c
- Uses null-terminated array pattern for valid methods list
- Terminates the entire initdb process on validation failure, ensuring no cluster is created with invalid authentication configuration
- Part of PostgreSQL's defensive programming approach to prevent insecure or non-functional configurations
- The valid methods array varies by connection type (local socket connections support different methods than TCP/IP connections)
- Error messages include both the invalid method and connection type for clear user feedback
- Critical for maintaining security and functionality standards in PostgreSQL installations