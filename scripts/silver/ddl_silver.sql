/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

USE DataWarehouse_HC;
IF OBJECT_ID('silver.Patient', 'U') IS NOT NULL
    DROP TABLE silver.Patient;
GO
CREATE TABLE silver.Patient (
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
IF OBJECT_ID('silver.Encounter', 'U') IS NOT NULL
    DROP TABLE silver.Encounter;
GO

CREATE TABLE silver.Encounter (
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
IF OBJECT_ID('silver.Claim', 'U') IS NOT NULL
    DROP TABLE silver.Claim;
GO
CREATE TABLE silver.Claim (
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
--support table for silver.Claim
IF OBJECT_ID('silver.ClaimItem', 'U') IS NOT NULL
    DROP TABLE silver.ClaimItem;
GO
CREATE TABLE silver.ClaimItem (
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
IF OBJECT_ID('silver.Observation', 'U') IS NOT NULL
    DROP TABLE silver.Observation;
GO

CREATE TABLE silver.Observation (
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

DROP TABLE IF EXISTS silver.Practitioner;
GO

CREATE TABLE silver.Practitioner (
	PractitionerKey		INT,
	PractitionerID		NVARCHAR(64),
	IdentifierValue     NVARCHAR(64),
	System				NVARCHAR(255),
	Prefix				NVARCHAR(50),
	GivenName			NVARCHAR(100),
	FamilyName			NVARCHAR(100),
	Email               NVARCHAR(255),
	Gender              NVARCHAR(20),
	AddressLine         NVARCHAR(255),
	City				NVARCHAR(100),
	State				NVARCHAR(100),
	PostalCode          NVARCHAR(20),
	UtilizationEncounters NVARCHAR(50),
	IsActive BIT
);
