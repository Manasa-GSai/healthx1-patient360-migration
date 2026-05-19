/*
 * WO-013: Canonical dim_encounter_type Reference Table — SQL Server DDL
 *
 * Resolves RTM-DRIFT-001/002: encounter type semantic drift between
 * billing (DECODE of visit_type_c) and care management (Caboodle PatientDim).
 * Single authoritative mapping used by all pipeline branches.
 *
 * Target: Curated_PRD.reference.dim_encounter_type
 * Replicated to: patient360_{env}.reference.dim_encounter_type (Delta)
 */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'reference')
    EXEC('CREATE SCHEMA [reference]');
GO

IF OBJECT_ID('reference.dim_encounter_type', 'U') IS NOT NULL
    DROP TABLE reference.dim_encounter_type;
GO

CREATE TABLE reference.dim_encounter_type (
    visit_type_c    INT           NOT NULL,
    encounter_type  VARCHAR(20)   NOT NULL,
    description     VARCHAR(100)  NOT NULL,
    effective_date  DATE          NOT NULL DEFAULT GETDATE(),
    is_active       BIT           NOT NULL DEFAULT 1,
    created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_dim_encounter_type PRIMARY KEY CLUSTERED (visit_type_c),
    CONSTRAINT CK_encounter_type_not_empty CHECK (LEN(encounter_type) > 0)
);
GO

CREATE INDEX IX_dim_encounter_type_active
    ON reference.dim_encounter_type (is_active)
    INCLUDE (visit_type_c, encounter_type);
GO

INSERT INTO reference.dim_encounter_type
    (visit_type_c, encounter_type, description, effective_date, is_active)
VALUES
    (101, 'OFFICE',    'Office visit — primary care or specialist', '2024-01-01', 1),
    (102, 'FOLLOWUP',  'Follow-up visit — post-procedure or post-discharge', '2024-01-01', 1),
    (103, 'WELLNESS',  'Annual wellness visit — preventive care', '2024-01-01', 1),
    (201, 'ACUTE',     'Acute care — urgent or emergency encounter', '2024-01-01', 1),
    (  0, 'OTHER',     'Unmapped encounter type — catch-all for unknown visit_type_c', '2024-01-01', 1);
GO

SELECT
    visit_type_c,
    encounter_type,
    description,
    effective_date,
    is_active
FROM reference.dim_encounter_type
ORDER BY visit_type_c;
GO
