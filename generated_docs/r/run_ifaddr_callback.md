# run_ifaddr_callback

## Location
[src/backend/libpq/ifaddr.c:181-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/ifaddr.c#L181-L229)

## Overview
Validates network interface address and mask parameters before executing a callback function for network interface processing.

## Definition


## Detailed Description
This static function serves as a wrapper that validates and sanitizes network interface address and mask data before invoking a user-provided callback function. It performs several validation checks: ensures the address is not NULL, verifies that the mask matches the address family, and checks that the mask is not an unspecified address (INADDR_ANY for IPv4 or unspecified for IPv6). If the provided mask is invalid or missing, the function generates a fully-set mask using pg_sockaddr_cidr_mask. This ensures that the callback always receives valid address and mask parameters.

## Parameters / Member Variables
- : Function pointer to the callback that will process the address and mask
- : User-provided data to be passed to the callback function
- : Network interface address to be processed
- : Network mask associated with the address (can be NULL or invalid)

## Dependencies
- Functions called/Symbols referenced:
  - [callback](../c/callback.md) (function pointer parameter)
  - [pg_sockaddr_cidr_mask](../p/pg_sockaddr_cidr_mask.md)
- Called from (representative examples):
  - [pg_foreach_ifaddr](../p/pg_foreach_ifaddr.md)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Performs address family validation between addr and mask
- Handles NULL address by returning early without calling the callback
- Generates a full mask (all bits set) when the provided mask is invalid
- Uses INADDR_ANY for IPv4 and IN6_IS_ADDR_UNSPECIFIED macro for IPv6 validation
- Acts as a safety layer to ensure callback functions receive consistent, valid data