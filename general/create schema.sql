-------------------------------------------------------
--------------------Schema creation--------------------
-------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION postgres;

CREATE SCHEMA IF NOT EXISTS silver
    AUTHORIZATION postgres;

CREATE SCHEMA IF NOT EXISTS gold
    AUTHORIZATION postgres;