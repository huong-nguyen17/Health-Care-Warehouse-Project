/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_patient
-- =============================================================================

--Create view for dim_patient table
IF OBJECT_ID('gold.dim_patient', 'V') IS NOT NULL
    DROP VIEW gold.dim_patient;
GO

CREATE  VIEW gold.dim_patient AS
SELECT
	ROW_NUMBER() OVER (ORDER BY [PatientID]) AS patient_key, --surrogate key
	[PatientID],
	[FamilyName],
	[GivenName],
	[Gender],
	[BirthDate],
	[MaritalStatus],
	[AddressLine],
	[City],
	[State],
	[Language],
	[DisabilityAdjustedLifeYears] AS Disability_years,
    [QualityAdjustedLifeYears] AS Quality_years
FROM [silver].[Patient]

--Create view for dim_practitioner table
IF OBJECT_ID('gold.dim_practitioner', 'V') IS NOT NULL
    DROP VIEW gold.dim_practitioner;
GO

CREATE  VIEW gold.dim_practitioner AS
SELECT
	[PractitionerKey] AS practitioner_key,
	[PractitionerID],
	[IdentifierValue],
	[System],
	[GivenName],
	[FamilyName],
	[FullName],
	[Email],
	[Gender],
	[AddressLine],
	[City],
	[State],
	[PostalCode],
	[Country],
	[UtilizationEncounters]

FROM [silver].[Practitioner]

--create fact_ClaimItem table
IF OBJECT_ID('gold.fact_claim', 'V') IS NOT NULL
    DROP VIEW gold.fact_claim;
GO

CREATE  VIEW gold.fact_claim AS
SELECT
	ROW_NUMBER() OVER (ORDER BY c.[ClaimID]) AS claim_key,--surrogate key
	c.[ClaimID],
	c.[PatientID],
	ci.EncounterID,
	c.[ClaimStatus],
	c.[ClaimType],
	ci.ItemSequence AS sequences,
	ci.ProductOrServiceCode AS code,
	ci.ProductOrServiceText AS product_service,
	c.[CreatedDate],			
	c.[BillablePeriodStart],
	c.[BillablePeriodEnd],
	c.[ProviderName],
	c.[PriorityCode],
	c.[InsuranceCoverage],
	c.[TotalBilled],	
	ci.NetValue
FROM [silver].[Claim] AS c
LEFT JOIN [silver].[ClaimItem] AS ci ON c.ClaimID= ci.ClaimID

--create fact_observation_encounter table
IF OBJECT_ID('gold.fact_observation_encounter', 'V') IS NOT NULL
    DROP VIEW gold.fact_observation_encounter;
GO

CREATE  VIEW gold.fact_observation_encounter AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY [ObservationID]) AS observation_key,--surrogate key
	o.[EncounterID]		
	,o.[PatientID]
	,[ObservationID]
	,e.EncounterClass
	,e.EncounterTypeCode as encounter_code
	,e.EncounterTypeText as encounter_text
	,[ObservationStatus]
	,[ObservationCategory]
	,[ObservationCode]
	,[ObservationText]
	,[Value]
	,[Unit]
	,e.PractitionerName
	,e.ServiceProvider
	,[EffectiveDate]
	,[IssuedDate]  
  FROM [DataWarehouse_HC].[silver].[Observation] o
  JOIN [silver].[Encounter] e ON o.EncounterID=e.EncounterID
