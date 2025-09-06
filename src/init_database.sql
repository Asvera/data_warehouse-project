
-- Create databases


-- Script Purpose:
--     This script creates a new database named 'DataWarehouse' after checking if it already exists. 
--     If the database exists, it is dropped and recreated. 
--     Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

DROP DATABASE IF EXISTS "DataWarehouse";
CREATE DATABASE "DataWarehouse";

