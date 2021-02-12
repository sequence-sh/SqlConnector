# EDR SQL Database Connector

A class library for building sequences that interact with SQL databases.

# How to Use

This connector contains six steps

## CreateConnectionString

|Step|Description|Result|
|-|-|-|
|CreateConnectionString|Creates a MsSql connection string|`String`|
|SqlCommand|Sends a command to a SQL database|`Unit`|
|SqlCreateSchemaFromTable|Creates a Schema entity from a SQL table|`Entity`|
|SqlCreateTable|Create a SQL table from a given schema|`Unit`|
|SqlInsert|Inserts data into a SQL table|`Unit`|
|SqlQuery|Executes a SQL query and returns the result as an entity stream.|`Array<Entity>`|


# Releases

Can be downloaded from the [Releases page](https://gitlab.com/reductech/edr/connectors/sql/-/releases).

# NuGet Packages

Are available for download from the [Releases page](https://gitlab.com/reductech/edr/connectors/sql/-/releases)
or from the `package nuget` jobs of the [CI pipelines](https://gitlab.com/reductech/edr/connectors/sql/-/pipelines). They're also available in:

- [Reductech Nuget feed](https://gitlab.com/reductech/nuget/-/packages) for releases
- [Reductech Nuget-dev feed](https://gitlab.com/reductech/nuget-dev/-/packages) for releases, master branch and dev builds
