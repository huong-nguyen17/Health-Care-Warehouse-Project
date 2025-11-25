/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 
 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Patient Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.Patient';
		TRUNCATE TABLE silver.Patient;
		PRINT '>> Inserting Data Into: silver.Patient';
		INSERT INTO silver.Patient (
			[PatientID]
			,[FamilyName]
			,[GivenName]
			,[Gender]
			,[BirthDate]
			,[MaritalStatus]
			,[AddressLine]
			,[City]
			,[State]
			,[PostalCode]
			,[Country]
			,[Latitude]
			,[Longitude]
			,[PhoneNumber]
			,[PhoneType]
			,[BirthplaceCity]
			,[BirthplaceState]
			,[BirthplaceCountry]
			,[MothersMaidenName]
			,[SSN]
			,[DriverLicense]
			,[PassportNumber]
			,[DisabilityAdjustedLifeYears]
			,[QualityAdjustedLifeYears]
			,[Language])		
		SELECT  
			[PatientID],
			LEFT(FamilyName, LEN(FamilyName) - 3) AS CleanFamilyName,
			LEFT(GivenName,  LEN(GivenName)  - 3) AS CleanGivenName, --Clean extra digits 
			[Gender],
			[BirthDate],
			CASE 
				WHEN UPPER(TRIM([MaritalStatus])) = 'M' THEN 'Married'
				WHEN UPPER(TRIM([MaritalStatus])) = 'S' THEN 'Single'
				ELSE [MaritalStatus] END AS [MaritalStatus],--Normalize marital status values to readable format
			[AddressLine],
			[City],
			[State],
			[PostalCode],
			[Country],
			[Latitude],
			[Longitude],
			[PhoneNumber],
			[PhoneType],
			[BirthplaceCity],
			[BirthplaceState],
			[BirthplaceCountry],
			LEFT(PARSENAME(REPLACE(MothersMaidenName, ' ', '.'), 2), LEN(PARSENAME(REPLACE(MothersMaidenName, ' ', '.'), 2)) - 3)
				+ ' ' +
			LEFT(PARSENAME(REPLACE(MothersMaidenName, ' ', '.'), 1), LEN(PARSENAME(REPLACE(MothersMaidenName, ' ', '.'), 1)) - 3)
				AS CleanMothersMaidenName,--Clean extra digits
			[SSN],
			COALESCE([DriverLicense],'N/A') AS [DriverLicense],
			COALESCE([PassportNumber],'N/A') AS [PassportNumber],
			ROUND([DisabilityAdjustedLifeYears],2) AS [DisabilityYears],
			ROUND([QualityAdjustedLifeYears],2) AS [QualityYears],
			[Language]
			FROM [DataWarehouse_HC].[bronze].[Patient]
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			--Clean Claim table
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Claim';
			TRUNCATE TABLE silver.Claim;
			PRINT '>> Inserting Data Into: silver.Claim';
			INSERT INTO silver.Claim (
				[ClaimID],
				[PatientID],
				[CreatedDate],
				[ClaimStatus],
				[ClaimType],
				[UseType],
				[BillablePeriodStart],
				[BillablePeriodEnd],
				[ProviderName],
				[PriorityCode],
				[InsuranceCoverage],
				[TotalBilled])			
			SELECT 
			[ClaimID],
			[PatientID],
			CAST([CreatedDate] AS DATE) AS [CreatedDate],--switch datetime to date
			[ClaimStatus],
			[ClaimType],
			[UseType],
			CAST([BillablePeriodStart] AS DATE) AS [BillablePeriodStart],
			CAST([BillablePeriodEnd] AS DATE) AS [BillablePeriodEnd] ,
			LOWER([ProviderName]) AS [ProviderName], --reformat the provider name
			[PriorityCode],
			[InsuranceCoverage],
			[TotalBilled]
			FROM [DataWarehouse_HC].[bronze].[Claim]
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			--clean ClaimItem table
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.ClaimItem';
			TRUNCATE TABLE silver.ClaimItem;
			PRINT '>> Inserting Data Into: silver.ClaimItem';
			INSERT INTO silver.ClaimItem (
				[ClaimID],
				[ItemSequence],
				[ProductOrServiceCode],
				[ProductOrServiceText],
				[EncounterID],
				[NetValue],
				[Currency])
			SELECT
				[ClaimID],
				[ItemSequence],
				[ProductOrServiceCode],
				[ProductOrServiceText],
				FIRST_VALUE([EncounterID]) OVER(PARTITION BY [ClaimID] ORDER BY [ItemSequence] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EncounterID,--fill N/A with encouterID
				COALESCE([NetValue], 0) AS [NetValue],
				COALESCE([Currency], 'USD') AS [Currency]
			FROM [DataWarehouse_HC].[bronze].[ClaimItem]
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Clean Encournter table

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Encounter';
			TRUNCATE TABLE silver.Encounter;
			PRINT '>> Inserting Data Into: silver.Encounter';
			INSERT INTO silver.Encounter (
				[EncounterID],
				[PatientID],
				[EncounterClass],
				[EncounterTypeCode],
				[EncounterTypeText],
				[Status],
				[StartDate],
				[EndDate],
				[PractitionerName],
				[ServiceProvider])
			SELECT 
				[EncounterID],
				[PatientID],
				[EncounterClass],
				[EncounterTypeCode],
				[EncounterTypeText],
				[Status],
				CAST([StartDate] AS DATE) AS StartDate, --convert datetime to date
				CAST([EndDate] AS DATE) AS EndDate,
				'Dr. ' +LEFT(PARSENAME(REPLACE([PractitionerName], ' ', '.'), 2), LEN(PARSENAME(REPLACE([PractitionerName], ' ', '.'), 2)) - 3)
				+ ' ' +
				LEFT(PARSENAME(REPLACE([PractitionerName], ' ', '.'), 1), LEN(PARSENAME(REPLACE([PractitionerName], ' ', '.'), 1)) - 3)--remove extra digits
				AS [PractitionerName],
				LOWER([ServiceProvider]) AS [ServiceProvider]
			FROM [DataWarehouse_HC].[bronze].[Encounter]
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			PRINT '>> -------------';
			--Clean Observation table
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Observation';
			TRUNCATE TABLE silver.Observation;
			PRINT '>> Inserting Data Into: silver.Observation';
			INSERT INTO silver.Observation (
				[ObservationID],
				[PatientID],
				[EncounterID],
				[ObservationStatus],
				[ObservationCategory],
				[ObservationCode],
				[ObservationText],
				[Value],
				[Unit],
				[UnitSystem],
				[UnitCode],
				[EffectiveDate],
				[IssuedDate])
			SELECT 
				[ObservationID],
				[PatientID],
				[EncounterID],
				[ObservationStatus],
				[ObservationCategory],
				[ObservationCode],
				[ObservationText],
				[Value],
				[Unit],
				[UnitSystem],
				[UnitCode],
				CAST([EffectiveDate] AS DATE) AS [EffectiveDate],
				CAST([IssuedDate] AS DATE) AS [IssuedDate]
			FROM [DataWarehouse_HC].[bronze].[Observation]
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			--Clean Pratitioner table
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Practitioner';
			TRUNCATE TABLE silver.Practitioner;
			PRINT '>> Inserting Data Into: silver.Practitioner';
			INSERT INTO silver.Practitioner (
				[PractitionerKey],
				[PractitionerID],
				[IdentifierValue],
				[System],
				[Prefix],
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
				[UtilizationEncounters],
				[IsActive]
			)
			SELECT 
				[PractitionerKey],
				[PractitionerID],
				[IdentifierValue],
				[System],
				[Prefix],
				LEFT(GivenName,  LEN(GivenName)  - 3) AS CleanGivenName,
				LEFT(FamilyName, LEN(FamilyName) - 3) AS CleanFamilyName,
				'Dr. ' +
				   LEFT(PARSENAME(REPLACE([FullName], ' ', '.'), 2),
						LEN(PARSENAME(REPLACE([FullName], ' ', '.'), 2)) - 3)
				   + ' ' +
				   LEFT(PARSENAME(REPLACE([FullName], ' ', '.'), 1),
						LEN(PARSENAME(REPLACE([FullName], ' ', '.'), 1)) - 3)
				   AS CleanFullName,
				[Email],
				[Gender],
				[AddressLine],
				[City],
				[State],
				[PostalCode],
				[Country],
				[UtilizationEncounters],
				[IsActive]
			FROM [DataWarehouse_HC].[bronze].[Practitioner];

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
