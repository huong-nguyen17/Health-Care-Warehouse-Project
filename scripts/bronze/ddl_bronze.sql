/*
========================================================================================
DDL Script:Create Bronze Tables
========================================================================================
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
Run this script to re-define the DDL structure of 'bronze' tables
========================================================================================
*/

USE DataWarehouse_HC;
IF OBJECT_ID('bronze.Patient', 'U') IS NOT NULL
    DROP TABLE bronze.Patient;
GO
CREATE TABLE bronze.Patient (
    PatientID                   NVARCHAR(64) PRIMARY KEY,
    FamilyName                  NVARCHAR(100),
    GivenName                   NVARCHAR(100),
    Gender                      NVARCHAR(20),
    BirthDate                   DATE,
    MaritalStatus               NVARCHAR(50),
    AddressLine                 NVARCHAR(200),
    City                        NVARCHAR(100),
    State                       NVARCHAR(100),
    PostalCode                  NVARCHAR(20),
    Country                     NVARCHAR(50),
    Latitude                    FLOAT,
    Longitude                   FLOAT,
    PhoneNumber                 NVARCHAR(50),
    PhoneType                   NVARCHAR(50),
    BirthplaceCity              NVARCHAR(100),
    BirthplaceState             NVARCHAR(100),
    BirthplaceCountry           NVARCHAR(100),
    MothersMaidenName           NVARCHAR(100),
    SSN                         NVARCHAR(50),
    DriverLicense               NVARCHAR(50),
    PassportNumber              NVARCHAR(50),
    DisabilityAdjustedLifeYears FLOAT,
    QualityAdjustedLifeYears    FLOAT,
    Language                    NVARCHAR(50),
  
);

---Encourter raw table import
IF OBJECT_ID('bronze.Encounter', 'U') IS NOT NULL
    DROP TABLE bronze.Encounter;
GO

CREATE TABLE bronze.Encounter (
    EncounterID        NVARCHAR(64),
    PatientID          NVARCHAR(64),
    EncounterClass     NVARCHAR(50),
    EncounterTypeCode  NVARCHAR(50),
    EncounterTypeText  NVARCHAR(300),
    Status             NVARCHAR(50),
    StartDate          DATETIME2,
    EndDate            DATETIME2,
    PractitionerName    NVARCHAR(200),
    ServiceProvider    NVARCHAR(200)
);
--Claim  table import
IF OBJECT_ID('bronze.Claim', 'U') IS NOT NULL
    DROP TABLE bronze.Claim;
GO
CREATE TABLE bronze.Claim (
    ClaimID             NVARCHAR(64),
    PatientID           NVARCHAR(64),
    CreatedDate         DATETIME2,
    ClaimStatus         NVARCHAR(50),
    ClaimType           NVARCHAR(50),
    UseType             NVARCHAR(50),
    BillablePeriodStart DATETIME2,
    BillablePeriodEnd   DATETIME2,
    ProviderName        NVARCHAR(200),
    PriorityCode        NVARCHAR(50),
    InsuranceCoverage   NVARCHAR(200),
    TotalBilled         DECIMAL(18,2)
);
GO
--support table for bronze.Claim
IF OBJECT_ID('bronze.ClaimItem', 'U') IS NOT NULL
    DROP TABLE bronze.ClaimItem;
GO
CREATE TABLE bronze.ClaimItem (
    ClaimID              NVARCHAR(64),
    ItemSequence         INT,
    ProductOrServiceCode NVARCHAR(50),
    ProductOrServiceText NVARCHAR(300),
    EncounterID          NVARCHAR(64),
    NetValue             DECIMAL(18,2),
    Currency             NVARCHAR(10)
);
GO
--Import Observation table
IF OBJECT_ID('bronze.Observation', 'U') IS NOT NULL
    DROP TABLE bronze.Observation;
GO

CREATE TABLE bronze.Observation (
    ObservationID         NVARCHAR(64),
    PatientID             NVARCHAR(64),
    EncounterID           NVARCHAR(64),
    ObservationStatus     NVARCHAR(50),
    ObservationCategory   NVARCHAR(100),
    ObservationCode       NVARCHAR(50),
    ObservationText       NVARCHAR(200),
    Value                 NVARCHAR(100),
    Unit                  NVARCHAR(50),
    UnitSystem            NVARCHAR(200),
    UnitCode              NVARCHAR(50),
    EffectiveDate         DATETIME2,
    IssuedDate            DATETIME2
);
GO
