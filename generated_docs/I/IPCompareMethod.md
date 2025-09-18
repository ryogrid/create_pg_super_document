# IPCompareMethod

## Location
src/include/libpq/hba.h: 55 - 56

## Overview
IPCompareMethod is an enumeration that defines different methods for comparing IP addresses in PostgreSQL's host-based authentication (HBA) system.

## Definition


## Detailed Description
This enum defines the various methods used to compare client IP addresses against the configured rules in pg_hba.conf. Each value represents a different approach to IP address matching:

- **ipCmpMask**: Compare using a netmask (CIDR notation matching)
- **ipCmpSameHost**: Match only the same host (exact IP match)
- **ipCmpSameNet**: Match addresses on the same network
- **ipCmpAll**: Match all addresses (no IP restriction)

The enum is used internally by PostgreSQL's authentication system to determine how to interpret and apply IP-based access rules defined in the pg_hba.conf configuration file.

## Parameters / Member Variables
- : Uses subnet mask comparison for IP matching
- : Performs exact IP address matching  
- : Matches addresses within the same network
- : Allows all IP addresses (no filtering)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - check_network_data (struct member)
  - check_same_host_or_net
  - HbaLine (struct member)

## Notes and Other Information
- Defined in src/include/libpq/hba.h:49-55
- Used as a member of the HbaLine structure to specify IP comparison method for each pg_hba.conf entry
- Critical component of PostgreSQL's host-based authentication system
- Works in conjunction with address and mask fields to implement IP-based access control