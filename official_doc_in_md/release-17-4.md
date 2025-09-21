E.2. Release 17.4  
---  
[Prev](release-17-5.md "E.1. Release 17.5") | [Up](release.md "Appendix E. Release Notes")| Appendix E. Release Notes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](release-17-3.md "E.3. Release 17.3")  
  
* * *

## E.2. Release 17.4 #

[E.2.1. Migration to Version 17.4](release-17-4.md#RELEASE-17-4-MIGRATION)
[E.2.2. Changes](release-17-4.md#RELEASE-17-4-CHANGES)

**Release date:** 2025-02-20

This release contains a few fixes from 17.3. For information about new features in major release 17, see [Section E.6](release-17.md "E.6. Release 17"). 

### E.2.1. Migration to Version 17.4 #

A dump/restore is not required for those running 17.X. 

However, if you are upgrading from a version earlier than 17.1, see [Section E.5](release-17-1.md "E.5. Release 17.1"). 

### E.2.2. Changes #

  * Improve behavior of libpq's quoting functions (Andres Freund, Tom Lane) [§](https://postgr.es/c/a92db3d02) [§](https://postgr.es/c/3abe6e04c) [§](https://postgr.es/c/3977bd298)

The changes made for CVE-2025-1094 had one serious oversight: `PQescapeLiteral()` and `PQescapeIdentifier()` failed to honor their string length parameter, instead always reading to the input string's trailing null. This resulted in including unwanted text in the output, if the caller intended to truncate the string via the length parameter. With very bad luck it could cause a crash due to reading off the end of memory. 

In addition, modify all these quoting functions so that when invalid encoding is detected, an invalid sequence is substituted for just the first byte of the presumed character, not all of it. This reduces the risk of problems if a calling application performs additional processing on the quoted string. 

  * Fix small memory leak in pg_createsubscriber (Ranier Vilela) [§](https://postgr.es/c/ff6d9cfcb)

  * Fix meson build system to correctly detect availability of the `bsd_auth.h` system header (Nazir Bilal Yavuz) [§](https://postgr.es/c/c9a1d2135)




* * *

[Prev](release-17-5.md "E.1. Release 17.5") | [Up](release.md "Appendix E. Release Notes")|  [Next](release-17-3.md "E.3. Release 17.3")  
---|---|---  
E.1. Release 17.5 | [Home](index.md "PostgreSQL 17.5 Documentation")|  E.3. Release 17.3
