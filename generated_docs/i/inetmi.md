# inetmi

## Location
[src/backend/utils/adt/network.c:2018-2094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L2018-L2094)

## Overview
A PostgreSQL built-in function that computes the difference between two inet addresses, returning a 64-bit signed integer representing the numerical distance between them.

## Definition
Datum inetmi(PG_FUNCTION_ARGS)

## Detailed Description
The `inetmi` function implements inet address subtraction by computing the numerical difference between two inet addresses of the same family (IPv4 or IPv6). It uses two's complement arithmetic to perform the subtraction, treating IP addresses as large binary integers and computing their difference byte by byte.

The function employs the traditional complement-increment-add approach: it complements the bits of the second address, adds 1 (handled by initializing carry to 1), and adds the result to the first address. This effectively computes `ip1 - ip2`.

The implementation includes comprehensive overflow detection for cases where the result exceeds the range of a 64-bit signed integer, and proper sign extension for addresses narrower than 64 bits.

## Parameters / Member Variables
- Function uses PostgreSQL's `PG_FUNCTION_ARGS` convention:
  - Argument 0: First inet address (accessed via `PG_GETARG_INET_PP`)
  - Argument 1: Second inet address to subtract (accessed via `PG_GETARG_INET_PP`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP
  - ip_family
  - ip_addrsize
  - ip_addr
  - ereport
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries using inet - inet operator

## Notes and Other Information
- Requires both inet addresses to be of the same family (IPv4 or IPv6)
- Uses two's complement arithmetic for robust subtraction across different address widths
- Handles overflow detection for results that exceed int64 range
- Performs proper sign extension for addresses smaller than 64 bits
- Supports the SQL `-` operator for inet - inet operations
- Returns a signed 64-bit integer that can be positive or negative depending on address ordering
- Part of PostgreSQL's network data type arithmetic infrastructure
- More complex than integer subtraction due to multi-byte address handling and overflow checking

## Simplified Source

```c
Datum inetmi(PG_FUNCTION_ARGS) {
    inet *address1 = PG_GETARG_INET_PP(0);
    inet *address2 = PG_GETARG_INET_PP(1);
    int64 result = 0;

    // Check that both addresses are the same family (IPv4 or IPv6)
    if (ip_family(address1) != ip_family(address2)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot subtract inet values of different sizes")));
    }

    // Perform two's complement subtraction: complement second address and add with carry=1
    int address_size = ip_addrsize(address1);
    unsigned char *bytes1 = ip_addr(address1);
    unsigned char *bytes2 = ip_addr(address2);
    int carry = 1;  // Start with 1 for two's complement increment
    int current_byte = 0;

    // Process bytes from least significant to most significant
    for (int byte_index = address_size - 1; byte_index >= 0; byte_index--) {
        // Two's complement: add first byte + complement of second byte + carry
        carry = bytes1[byte_index] + (~bytes2[byte_index] & 0xFF) + carry;
        int result_byte = carry & 0xFF;

        // Build result as 64-bit integer (if it fits)
        if (current_byte < sizeof(int64)) {
            result |= ((int64) result_byte) << (current_byte * 8);
        } else {
            // Check for overflow in bytes beyond 64-bit capacity
            bool expected_high_byte = (result < 0) ? (result_byte == 0xFF) : (result_byte == 0);
            if (!expected_high_byte) {
                ereport(ERROR,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("result is out of range")));
            }
        }

        carry >>= 8;
        current_byte++;
    }

    // Sign extend for addresses narrower than 64 bits
    if (carry == 0 && current_byte < sizeof(int64)) {
        result |= ((uint64) (int64) -1) << (current_byte * 8);
    }

    PG_RETURN_INT64(result);
}
```