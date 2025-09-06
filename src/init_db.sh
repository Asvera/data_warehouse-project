#!/bin/bash


# The DROP DATABASE and CREATE DATABASE commands cannot be run inside a transaction block.
# Also, you cannot run CREATE SCHEMA commands right after creating the database in the same connection/session because:
# Once you create a new database, you need to connect to that new database to create schemas there.
# CREATE SCHEMA runs inside a specific database context, not the server/global context.

# Thus for this solution create 2 scripts to create db and schema seprately

psql -U postgres -d postgres  -h 127.0.0.1 -f init_database.sql
psql -U postgres -d DataWarehouse  -h 127.0.0.1 -f create_schemas.sql
