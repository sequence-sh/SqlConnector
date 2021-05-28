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
