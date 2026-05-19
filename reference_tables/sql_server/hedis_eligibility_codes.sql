/*
 * WO-015: Normalized HEDIS Eligibility Code Set Table — SQL Server DDL
 *
 * Resolves RTM-HEDIS-001: INSTR substring matching against
 * comma-delimited CPT/ICD-10 strings caused false-positive matches
 * (e.g., CPT '9921' matching inside '99213').
 *
 * This table normalizes the delimited eligibility_cpt_set and
 * eligibility_icd10_set from HEDIS_MeasureDefinition into one row
 * per measure_id + code combination, enabling exact-match joins.
 *
 * Target: Curated_PRD.reference.hedis_eligibility_codes
 * Replicated to: patient360_{env}.reference.hedis_eligibility_codes (Delta)
 */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'reference')
    EXEC('CREATE SCHEMA [reference]');
GO

IF OBJECT_ID('reference.hedis_eligibility_codes', 'U') IS NOT NULL
    DROP TABLE reference.hedis_eligibility_codes;
GO

CREATE TABLE reference.hedis_eligibility_codes (
    measure_id       VARCHAR(10)   NOT NULL,
    code_type        VARCHAR(10)   NOT NULL,
    code_value       VARCHAR(10)   NOT NULL,
    effective_date   DATE          NOT NULL DEFAULT '2024-01-01',
    expiration_date  DATE          NULL,
    created_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_hedis_eligibility_codes
        PRIMARY KEY CLUSTERED (measure_id, code_type, code_value),
    CONSTRAINT CK_code_type
        CHECK (code_type IN ('CPT', 'ICD10')),
    CONSTRAINT CK_code_value_not_empty
        CHECK (LEN(code_value) > 0),
    CONSTRAINT CK_code_no_delimiters
        CHECK (code_value NOT LIKE '%,%' AND code_value NOT LIKE '% %')
);
GO

CREATE INDEX IX_hedis_code_lookup
    ON reference.hedis_eligibility_codes (code_type, code_value)
    INCLUDE (measure_id);
GO

/*
 * ETL: Normalize comma-delimited strings from HEDIS_MeasureDefinition.
 *
 * Source: dbo.HEDIS_MeasureDefinition.eligibility_cpt_set (comma-delimited CPT codes)
 *         dbo.HEDIS_MeasureDefinition.eligibility_icd10_set (comma-delimited ICD-10 codes)
 *
 * Uses STRING_SPLIT (SQL Server 2016+) to explode delimited strings.
 * LTRIM/RTRIM handles any whitespace around delimiters.
 */

INSERT INTO reference.hedis_eligibility_codes
    (measure_id, code_type, code_value, effective_date)
SELECT
    md.measure_id,
    'CPT' AS code_type,
    LTRIM(RTRIM(codes.value)) AS code_value,
    CAST('2024-01-01' AS DATE) AS effective_date
FROM dbo.HEDIS_MeasureDefinition md
CROSS APPLY STRING_SPLIT(md.eligibility_cpt_set, ',') codes
WHERE LTRIM(RTRIM(codes.value)) <> ''

UNION ALL

SELECT
    md.measure_id,
    'ICD10' AS code_type,
    LTRIM(RTRIM(codes.value)) AS code_value,
    CAST('2024-01-01' AS DATE) AS effective_date
FROM dbo.HEDIS_MeasureDefinition md
CROSS APPLY STRING_SPLIT(md.eligibility_icd10_set, ',') codes
WHERE LTRIM(RTRIM(codes.value)) <> '';
GO

/* Validation: count per measure should match count of delimited values in source */
SELECT
    hec.measure_id,
    hec.code_type,
    COUNT(*) AS normalized_count
FROM reference.hedis_eligibility_codes hec
GROUP BY hec.measure_id, hec.code_type
ORDER BY hec.measure_id, hec.code_type;
GO

/* Validation: no partial/substring codes — all code_value lengths are valid */
SELECT
    code_type,
    code_value,
    LEN(code_value) AS code_length
FROM reference.hedis_eligibility_codes
WHERE (code_type = 'CPT' AND LEN(code_value) < 4)
   OR (code_type = 'ICD10' AND LEN(code_value) < 3);
GO
