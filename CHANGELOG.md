# v0.18.0 (2022-11-14)

## Summary of Changes

- Sequence has a new home: https://gitlab.com/sequence
- The namespace has been updated from `Reductech.Sequence` to `Sequence`

## Issues Closed in this Release

### Bug Fixes

- The type initializer for LogSituationSql threw an exception #163

### Other

- Update namespace and paths after move to Sequence group #160

# v0.17.0 (2022-08-29)

Switched to using the `System.Data.SqlClient` package as `Microsoft.Data.SqlClient` package
was causing issues with JSON dependencies when packaged as a connector.

Added additional parameters to `CreateMsSQLConnectionString` to allow unencrypted and local
connections.

## Summary of Changes

### Connector Updates

- Added additional parameters to `CreateMsSQLConnectionString`
   - AttachDBFilename
   - Authentication
   - Encrypt
   - Integrated Security
   - TrustServerCertificate

## Issues Closed in this Release

### New Features

- Add additional parameters to CreateMsSQLConnectionString #128

### Bug Fixes

- ToJsonElement MissingMethodException #132

### Other

- Change reference to Microsoft.Data.SqlClient to System.Data.SqlClient #133

# v0.16.0 (2022-07-13)

- Enabled [Source Link](https://docs.microsoft.com/en-us/dotnet/standard/library-guidance/sourcelink)
- Enabled publish to [Nuget.org](https://www.nuget.org) including symbols
- Update Core to v0.16.0

# v0.15.0 (2022-05-27)

Maintenance release - dependency updates only.

# v0.14.0 (2022-03-25)

Maintenance release - dependency updates only.

# v0.13.0 (2022-01-16)

EDR is now Sequence. The following has changed:

- The GitLab group has moved to https://gitlab.com/reductech/sequence
- The root namespace is now `Reductech.Sequence`
- The documentation site has moved to https://sequence.sh

Everything else is still the same - automation, simplified.

The project has now been updated to use .NET 6.

## Issues Closed in this Release

### Maintenance

- Rename EDR to Sequence #41
- Update Core to support SCLObject types #37
- Upgrade to use .net 6 #36

# v0.12.1 (2021-12-01)

Bug fix release.

## Issues Closed in this Release

### Bug Fixes

- Explicity reference System.Text.Json and exclude assets
  #33

# v0.12.0 (2021-11-26)

Maintenance release - dependency updates only.

# v0.11.0 (2021-09-03)

Dependency updates only

# v0.10.0 (2021-07-02)

## Issues Closed in this Release

### Maintenance

- Update Core to latest and remove SCLSettings #20

# v0.9.1 (2021-05-28)

Fix issues with using MS SQL when packaged as a connector.

## Issues Closed in this Release

### Bug Fixes

- Running from edr throws 'System.Data.SqlClient is not supported on this platform' #18
- CreateMySQLConnectionString returns the wrong database type #16

# v0.9.0 (2021-05-14)

## Summary of Changes

### Core SDK

- Connector can now be used as a plugin for EDR

### Connector Updates

- Steps in the SQL connector now reuse the most recent connection by default.

## Issues Closed in this Release

### New Features

- Change SQL connection management to make SCL more concise and easier for technicians #12
- Allow this package to be used as a plugin #11

### Maintenance

- Enable publish to connector registry #15
- Update Core dependecies #14
- Improve coverage by creating more unit tests #13

# v0.8.0 (2021-04-08)

- Update of Core dependencies only

# v0.7.0 (2021-03-26)

- Update of Core dependencies only

# v0.6.0 (2021-03-14)

- Update of Core dependencies only

# v0.5.0 (2021-03-01)

## Summary of Changes

First release of the SQL Connector. Versions are aligned with Core.

This connector supports:

- MariaDB
- MS SQL Server
- MySQL
- PostgresSQL

### Steps

- Added
  - CreateConnectionString
  - SqlCreateSchemaFromTable
  - SqlCreateTable
  - SqlInsert
  - SqlQuery
- Added SqlCommand

## Issues Closed in this Release

### New Features

- Add support for postgresql and mariadb #4
- Create Step to insert entity stream into a db table #3
- Create Step to run SQL query and return result as entity, so that technicians can use database data in sequences #1
- Create Step to execute SQL queries that do not return a result #2

### Maintenance

- Update version of Core to support enhanced logging #5

