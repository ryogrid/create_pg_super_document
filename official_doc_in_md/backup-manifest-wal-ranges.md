69.3. Backup Manifest WAL Range Object  
---  
[Prev](backup-manifest-files.md "69.2. Backup Manifest File Object") | [Up](backup-manifest-format.md "Chapter 69. Backup Manifest Format")| Chapter 69. Backup Manifest Format| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](appendixes.md "Part VIII. Appendixes")  
  
* * *

## 69.3. Backup Manifest WAL Range Object #

The object which describes a WAL range always has three keys: 

`Timeline`
    

The timeline for this range of WAL records, as an integer. 

`Start-LSN`
    

The LSN at which replay must begin on the indicated timeline in order to make use of this backup. The LSN is stored in the format normally used by PostgreSQL; that is, it is a string consisting of two strings of hexadecimal characters, each with a length of between 1 and 8, separated by a slash. 

`End-LSN`
    

The earliest LSN at which replay on the indicated timeline may end when making use of this backup. This is stored in the same format as `Start-LSN`. 

Ordinarily, there will be only a single WAL range. However, if a backup is taken from a standby which switches timelines during the backup due to an upstream promotion, it is possible for multiple ranges to be present, each with a different timeline. There will never be multiple WAL ranges present for the same timeline. 

* * *

[Prev](backup-manifest-files.md "69.2. Backup Manifest File Object") | [Up](backup-manifest-format.md "Chapter 69. Backup Manifest Format")|  [Next](appendixes.md "Part VIII. Appendixes")  
---|---|---  
69.2. Backup Manifest File Object | [Home](index.md "PostgreSQL 17.5 Documentation")|  Part VIII. Appendixes
