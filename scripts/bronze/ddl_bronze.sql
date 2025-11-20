/*
=========================================================================================
Stored Procedure: Load Bronze Layer (Raw_FHIR_Bundle -> Bronze)
==========================================================================================
Script Purpose:
  This store procedure loads data into the bronze schema from Raw_FHIR_Bundle.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses Insert statement to load data to tables from JSON format.
Parameters:
  None.
This store procedure does not accept any parameteres or return any values.

Usage example:
  EXEC.bronze.load_bronze;
==========================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_starttime DATETIME, @batch_endtime DATETIME;
	BEGIN TRY
		SET @batch_starttime = GETDATE();
		PRINT '=====================================================================';
		PRINT 'Loading Procedure';
		PRINT '=====================================================================';

		PRINT '------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Patient';
		TRUNCATE TABLE bronze.Patient;

--Patient table
		PRINT '>> Inserting Data Into: bronze.Patient';
		INSERT INTO [bronze].[Patient](
			PatientID, FamilyName, GivenName, Gender, BirthDate, MaritalStatus,
			AddressLine, City, State, PostalCode, Country,
			Latitude, Longitude,
			PhoneNumber, PhoneType,
			BirthplaceCity, BirthplaceState, BirthplaceCountry,
			MothersMaidenName, SSN, DriverLicense, PassportNumber,
			DisabilityAdjustedLifeYears, QualityAdjustedLifeYears,
			Language)
		SELECT
			JSON_VALUE(ResourceJSON,'$.id') AS PatientID,
			JSON_VALUE(ResourceJSON,'$.name[0].family') AS FamilyName,
			JSON_VALUE(ResourceJSON,'$.name[0].given[0]') AS GivenName,
			JSON_VALUE(ResourceJSON,'$.gender') AS Gender,
			TRY_CONVERT(date, JSON_VALUE(ResourceJSON,'$.birthDate')) AS BirthDate,
			JSON_VALUE(ResourceJSON,'$.maritalStatus.text') AS MaritalStatus,

			JSON_VALUE(ResourceJSON,'$.address[0].line[0]') AS AddressLine,
			JSON_VALUE(ResourceJSON,'$.address[0].city') AS City,
			JSON_VALUE(ResourceJSON,'$.address[0].state') AS State,
			JSON_VALUE(ResourceJSON,'$.address[0].postalCode') AS PostalCode,
			JSON_VALUE(ResourceJSON,'$.address[0].country') AS Country,

			geoLoc.Latitude,
			geoLoc.Longitude,

			JSON_VALUE(ResourceJSON,'$.telecom[0].value') AS PhoneNumber,
			JSON_VALUE(ResourceJSON,'$.telecom[0].system') AS PhoneType,

			bp.BirthplaceCity,
			bp.BirthplaceState,
			bp.BirthplaceCountry,

			mmn.MothersMaidenName,
			ssn.SSN,
			dl.DriverLicense,
			ppn.PassportNumber,

			daly.DalyValue AS DisabilityAdjustedLifeYears,
			qaly.QalyValue AS QualityAdjustedLifeYears,

			JSON_VALUE(ResourceJSON,'$.communication[0].language.text') AS Language

		FROM Raw_FHIR_Bundle
		OUTER APPLY (
			SELECT
				TRY_CONVERT(float,
					(SELECT JSON_VALUE(child.value,'$.valueDecimal')
					 FROM OPENJSON(parent.value,'$.extension') child
					 WHERE JSON_VALUE(child.value,'$.url')='latitude')
				) AS Latitude,
				TRY_CONVERT(float,
					(SELECT JSON_VALUE(child.value,'$.valueDecimal')
					 FROM OPENJSON(parent.value,'$.extension') child
					 WHERE JSON_VALUE(child.value,'$.url')='longitude')
				) AS Longitude
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.address[0].extension')) parent
			WHERE JSON_VALUE(parent.value,'$.url')=
				  'http://hl7.org/fhir/StructureDefinition/geolocation'
		) geoLoc
		OUTER APPLY (
			SELECT
				JSON_VALUE(ext.value,'$.valueAddress.city') AS BirthplaceCity,
				JSON_VALUE(ext.value,'$.valueAddress.state') AS BirthplaceState,
				JSON_VALUE(ext.value,'$.valueAddress.country') AS BirthplaceCountry
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.extension')) ext
			WHERE JSON_VALUE(ext.value,'$.url') LIKE '%birthPlace'
		) bp
		OUTER APPLY (
			SELECT JSON_VALUE(ext.value,'$.valueString') AS MothersMaidenName
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.extension')) ext
			WHERE JSON_VALUE(ext.value,'$.url') LIKE '%mothersMaidenName%'
		) mmn
		OUTER APPLY (
			SELECT JSON_VALUE(id.value,'$.value') AS SSN
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.identifier')) id
			WHERE JSON_VALUE(id.value,'$.type.coding[0].code')='SS'
		) ssn

		OUTER APPLY (
			SELECT JSON_VALUE(id.value,'$.value') AS DriverLicense
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.identifier')) id
			WHERE JSON_VALUE(id.value,'$.type.coding[0].code')='DL'
		) dl

		OUTER APPLY (
			SELECT JSON_VALUE(id.value,'$.value') AS PassportNumber
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.identifier')) id
			WHERE JSON_VALUE(id.value,'$.type.coding[0].code')='PPN'
		) ppn
		OUTER APPLY (
			SELECT TRY_CONVERT(float, JSON_VALUE(ext.value,'$.valueDecimal')) AS DalyValue
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.extension')) ext
			WHERE JSON_VALUE(ext.value,'$.url') LIKE '%disability-adjusted-life-years%'
		) daly

		OUTER APPLY (
			SELECT TRY_CONVERT(float, JSON_VALUE(ext.value,'$.valueDecimal')) AS QalyValue
			FROM OPENJSON(JSON_QUERY(ResourceJSON,'$.extension')) ext
			WHERE JSON_VALUE(ext.value,'$.url') LIKE '%quality-adjusted-life-years%'
		) qaly

		WHERE ResourceType='Patient';
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

--Encounter table
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Encounter';
		TRUNCATE TABLE bronze.Encounter;

		PRINT '>> Inserting Data Into: bronze.Encounter';
		INSERT INTO [bronze].[Encounter] (
			EncounterID,
			PatientID,
			EncounterClass,
			EncounterTypeCode,
			EncounterTypeText,
			Status,
			StartDate,
			EndDate,
			PractitionerName,
			ServiceProvider
		)
		SELECT
			JSON_VALUE(ResourceJSON,'$.id') AS EncounterID,

			REPLACE(JSON_VALUE(ResourceJSON,'$.subject.reference'), 'urn:uuid:', '')
				AS PatientID,

			JSON_VALUE(ResourceJSON,'$.class.code') AS EncounterClass,

			JSON_VALUE(ResourceJSON,'$.type[0].coding[0].code') AS EncounterTypeCode,

			JSON_VALUE(ResourceJSON,'$.type[0].text') AS EncounterTypeText,

			JSON_VALUE(ResourceJSON,'$.status') AS Status,

			TRY_CONVERT(datetime2, JSON_VALUE(ResourceJSON,'$.period.start')) AS StartDate,
			TRY_CONVERT(datetime2, JSON_VALUE(ResourceJSON,'$.period.end')) AS EndDate,

			JSON_VALUE(ResourceJSON,'$.participant[0].individual.display') AS PractitionerName,

			JSON_VALUE(ResourceJSON,'$.serviceProvider.display') AS ServiceProvider

		FROM Raw_FHIR_Bundle
		WHERE ResourceType='Encounter';
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		--Observation table
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Observation';
		TRUNCATE TABLE bronze.Observation;

		PRINT '>> Inserting Data Into: bronze.Observation';
		INSERT INTO [bronze].[Observation](
			ObservationID,
			PatientID,
			EncounterID,
			ObservationStatus,
			ObservationCategory,
			ObservationCode,
			ObservationText,
			Value,
			Unit,
			UnitSystem,
			UnitCode,
			EffectiveDate,
			IssuedDate
		)
		SELECT
			JSON_VALUE(ResourceJSON,'$.id') AS ObservationID,

			REPLACE(JSON_VALUE(ResourceJSON,'$.subject.reference'), 'urn:uuid:', '')
				AS PatientID,

			REPLACE(JSON_VALUE(ResourceJSON,'$.encounter.reference'), 'urn:uuid:', '')
				AS EncounterID,

			JSON_VALUE(ResourceJSON,'$.status') AS ObservationStatus,

			JSON_VALUE(ResourceJSON,'$.category[0].coding[0].display') AS ObservationCategory,

			JSON_VALUE(ResourceJSON,'$.code.coding[0].code') AS ObservationCode,

			COALESCE(
				JSON_VALUE(ResourceJSON,'$.code.text'),
				JSON_VALUE(ResourceJSON,'$.code.coding[0].display')
			) AS ObservationText,

			-- Handles numeric and coded/string value types
			COALESCE(
				JSON_VALUE(ResourceJSON,'$.valueQuantity.value'),
				JSON_VALUE(ResourceJSON,'$.valueString'),
				JSON_VALUE(ResourceJSON,'$.valueCodeableConcept.text')
			) AS Value,

			JSON_VALUE(ResourceJSON,'$.valueQuantity.unit') AS Unit,
			JSON_VALUE(ResourceJSON,'$.valueQuantity.system') AS UnitSystem,
			JSON_VALUE(ResourceJSON,'$.valueQuantity.code') AS UnitCode,

			TRY_CONVERT(datetime2, JSON_VALUE(ResourceJSON,'$.effectiveDateTime')) AS EffectiveDate,
			TRY_CONVERT(datetime2, JSON_VALUE(ResourceJSON,'$.issued')) AS IssuedDate

		FROM Raw_FHIR_Bundle
		WHERE ResourceType='Observation';
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		--Claim table
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Claim';
		TRUNCATE TABLE bronze.Claim;

		PRINT '>> Inserting Data Into: bronze.Claim';
		INSERT INTO [bronze].[Claim] (
			ClaimID,
			PatientID,
			CreatedDate,
			ClaimStatus,
			ClaimType,
			UseType,
			BillablePeriodStart,
			BillablePeriodEnd,
			ProviderName,
			PriorityCode,
			InsuranceCoverage,
			TotalBilled
		)
		SELECT
			JSON_VALUE(ResourceJSON, '$.id') AS ClaimID,
			REPLACE(JSON_VALUE(ResourceJSON, '$.patient.reference'), 'urn:uuid:', '') AS PatientID,
			TRY_CONVERT(datetime2, JSON_VALUE(ResourceJSON, '$.created')) AS CreatedDate,
			JSON_VALUE(ResourceJSON, '$.status') AS ClaimStatus,
			JSON_VALUE(ResourceJSON, '$.type.coding[0].code') AS ClaimType,
			JSON_VALUE(ResourceJSON, '$.use') AS UseType,
			TRY_CONVERT(datetime2, JSON_VALUE(ResourceJSON, '$.billablePeriod.start')) AS BillablePeriodStart,
			TRY_CONVERT(datetime2, JSON_VALUE(ResourceJSON, '$.billablePeriod.end')) AS BillablePeriodEnd,
			JSON_VALUE(ResourceJSON, '$.provider.display') AS ProviderName,
			JSON_VALUE(ResourceJSON, '$.priority.coding[0].code') AS PriorityCode,
			JSON_VALUE(ResourceJSON, '$.insurance[0].coverage.display') AS InsuranceCoverage,
			TRY_CONVERT(decimal(18,2), JSON_VALUE(ResourceJSON, '$.total.value')) AS TotalBilled
		FROM Raw_FHIR_Bundle
		WHERE ResourceType = 'Claim';
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		--ClaimItem table
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.ClaimItem';
		TRUNCATE TABLE bronze.ClaimItem;

		PRINT '>> Inserting Data Into: bronze.ClaimItem';
		INSERT INTO [bronze].[ClaimItem] (
			ClaimID,
			ItemSequence,
			ProductOrServiceCode,
			ProductOrServiceText,
			EncounterID,
			NetValue,
			Currency
		)
		SELECT
			JSON_VALUE(ResourceJSON, '$.id') AS ClaimID,
			JSON_VALUE(item.value, '$.sequence') AS ItemSequence,
			JSON_VALUE(item.value, '$.productOrService.coding[0].code') AS ProductOrServiceCode,
			JSON_VALUE(item.value, '$.productOrService.text') AS ProductOrServiceText,
			REPLACE(JSON_VALUE(item.value, '$.encounter[0].reference'), 'urn:uuid:', '') AS EncounterID,
			TRY_CONVERT(decimal(18,2), JSON_VALUE(item.value, '$.net.value')) AS NetValue,
			JSON_VALUE(item.value, '$.net.currency') AS Currency
		FROM Raw_FHIR_Bundle
		CROSS APPLY OPENJSON(ResourceJSON, '$.item') AS item
		WHERE ResourceType = 'Claim';
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		--Practitioner table
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Practitioner';
		TRUNCATE TABLE bronze.Practitioner;

		PRINT '>> Inserting Data Into: bronze.Practitioner';
		INSERT INTO  [bronze].[Practitioner](
			PractitionerID, IdentifierValue, System,
			Prefix, GivenName, FamilyName,
			Email, Gender, AddressLine, City, State, PostalCode, Country,
			UtilizationEncounters, IsActive
		)
		SELECT
			JSON_VALUE(ResourceJSON, '$.id'),
			JSON_VALUE(ResourceJSON, '$.identifier[0].value'),
			JSON_VALUE(ResourceJSON, '$.identifier[0].system'),

			JSON_VALUE(ResourceJSON, '$.name[0].prefix[0]'),
			JSON_VALUE(ResourceJSON, '$.name[0].given[0]'),
			JSON_VALUE(ResourceJSON, '$.name[0].family'),

			JSON_VALUE(ResourceJSON, '$.telecom[0].value'),
			JSON_VALUE(ResourceJSON, '$.gender'),

			JSON_VALUE(ResourceJSON, '$.address[0].line[0]'),
			JSON_VALUE(ResourceJSON, '$.address[0].city'),
			JSON_VALUE(ResourceJSON, '$.address[0].state'),
			JSON_VALUE(ResourceJSON, '$.address[0].postalCode'),
			JSON_VALUE(ResourceJSON, '$.address[0].country'),

			JSON_VALUE(ResourceJSON, '$.extension[0].valueInteger'),
			JSON_VALUE(ResourceJSON, '$.active')
		FROM Raw_FHIR_Bundle
		WHERE ResourceType = 'Practitioner';
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'
		END TRY
	BEGIN CATCH
		PRINT '==========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================================';
	END CATCH

END
