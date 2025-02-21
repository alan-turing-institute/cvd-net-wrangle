-- DDL for CVD-Net consolidated database
-- Separate schema for the de-identified version of the database

CREATE SCHEMA cvdnet_consolidated;

-- Table of datasets
CREATE TABLE cvdnet_consolidated.datasets (
    id serial PRIMARY KEY,
    dataset_name varchar UNIQUE NOT NULL,
    date_last_updated timestamptz NOT NULL DEFAULT NOW()
);

-- Annotations for harmonising variables
-- This table will likely need expanding later on in the project (e.g. knowledge graph work)
CREATE TABLE cvdnet_consolidated.annotations (
    id serial PRIMARY KEY,
    category_level_1 varchar not null, -- Highest level harmonisation category
    category_level_2 varchar, -- More detailed category (a sub category of level 1)
    category_description varchar,
    date_last_updated timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE(category_level_1, category_level_2)
);

-- Table of variable metadata derived from data dictionaries
CREATE TABLE cvdnet_consolidated.metadata_variables (
    id serial PRIMARY KEY,
    dataset_id integer REFERENCES cvdnet_consolidated.datasets(id) NOT NULL, -- Could this be null for derived variables?
    variable_name varchar UNIQUE NOT NULL,
    variable_description varchar,
    data_type varchar, 
    unit varchar,
    associated_visit varchar, -- e.g. if variable is baseline, 1 year, etc.
    category_id integer REFERENCES cvdnet_consolidated.annotations(id), 
    has_options boolean NOT NULL,
    range_min decimal,
    range_max decimal,
    deidentification_required boolean NOT NULL,
    deidentification_method varchar,
    variable_source varchar CHECK (variable_source IN ('ORIGINAL', 'DERIVED')), -- For identifying variables that we have derived vs those provided to us
    date_last_updated timestamptz NOT NULL DEFAULT NOW()
);

-- Table of options associated with specific variables
CREATE TABLE cvdnet_consolidated.metadata_variable_options (
    id serial PRIMARY KEY,
    variable_id integer REFERENCES cvdnet_consolidated.metadata_variables(id) NOT NULL,
    option_name varchar NOT NULL,
    option_description varchar,
    date_last_updated timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (variable_id, option_name)
);

-- Subject table for static demographic data that should not change
CREATE TABLE cvdnet_consolidated.subjects (
    id serial PRIMARY KEY,
    dataset_id integer REFERENCES cvdnet_consolidated.datasets(id),
    subject_identifier varchar not null,
    subject_identifier_deid varchar UNIQUE not null,
    gender varchar CHECK (gender IN ('M', 'F')), -- can add other genders if required
    date_of_birth date, -- recorded as provided by provider
    date_of_birth_deid date,
    date_of_death date,
    date_of_death_deid date,
    ethnicity varchar, -- can add harmonised categories here
    date_last_updated timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE(dataset_id, subject_identifier) -- In case different datasets use the same identifier
    );

-- Table for recording all measurements in long format
CREATE TABLE cvdnet_consolidated.measurements (
    id serial PRIMARY KEY,
    subject_id integer REFERENCES cvdnet_consolidated.subjects(id) NOT NULL,
    variable_id integer REFERENCES cvdnet_consolidated.metadata_variables(id) NOT NULL,
    measurement_date date,
    measurement_time time,
    visit_grouping varchar, -- for adding details such as 'Baseline', 'Week 1'
    value varchar, -- varchar so all values in same column. Data type validation needs to occur in pipeline
    value_deid varchar, -- a de-identified version of the value column
    date_last_updated timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (subject_id, measurement_date, measurement_time, variable_id) -- added to stop duplicate being added.
);

-- VIEWS --
-- Views created to make interacting with the database easier for end users
-- These views will need to be modified and new views created to meet researcher needs
-- May need to pivot some of the long data to wide
CREATE VIEW cvdnet_consolidated.view_metadata AS
    SELECT
        dataset_name,
        variable_name,
        variable_description,
        data_type,
        unit,
        category_level_1,
        category_level_2,
        option_name,
        option_description
    FROM cvdnet_consolidated.metadata_variables as v
    LEFT JOIN cvdnet_consolidated.metadata_variable_options as vo on vo.variable_id = v.id
    LEFT JOIN cvdnet_consolidated.datasets as d on d.id = v.dataset_id
    LEFT JOIN cvdnet_consolidated.annotations as c on c.id = v.category_id
; 

CREATE VIEW cvdnet_consolidated.view_subject_measurements AS
    SELECT
        dataset_name,
        subject_identifier_deid,
        date_of_birth_deid,
        date_of_death_deid,
        gender,
        measurement_date,
        measurement_time,
        visit_grouping,
        variable_name,
        value_deid,
        unit
    FROM cvdnet_consolidated.subjects as s
    LEFT JOIN cvdnet_consolidated.datasets as d on d.id = s.dataset_id
    LEFT JOIN cvdnet_consolidated.measurements as m on m.subject_id = s.id
    LEFT JOIN cvdnet_consolidated.metadata_variables as v on v.id = m.variable_id
;

-- TRIGGERS --
-- Function and triggers for auto-updating last_updated_columns
CREATE FUNCTION cvdnet_consolidated.trigger_last_updated()
RETURNS TRIGGER as $$
BEGIN
    NEW.date_last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER last_updated_datasets
    BEFORE UPDATE
    ON cvdnet_consolidated.datasets
    FOR EACH ROW
    EXECUTE PROCEDURE cvdnet_consolidated.trigger_last_updated();

CREATE TRIGGER last_updated_annotations
    BEFORE UPDATE
    ON cvdnet_consolidated.annotations
    FOR EACH ROW
    EXECUTE PROCEDURE cvdnet_consolidated.trigger_last_updated();

CREATE TRIGGER last_updated_metadata_variables
    BEFORE UPDATE
    ON cvdnet_consolidated.metadata_variables
    FOR EACH ROW
    EXECUTE PROCEDURE cvdnet_consolidated.trigger_last_updated();

CREATE TRIGGER last_updated_metadata_variable_options
    BEFORE UPDATE
    ON cvdnet_consolidated.metadata_variable_options
    FOR EACH ROW
    EXECUTE PROCEDURE cvdnet_consolidated.trigger_last_updated();

CREATE TRIGGER last_updated_subjects
    BEFORE UPDATE
    ON cvdnet_consolidated.subjects
    FOR EACH ROW
    EXECUTE PROCEDURE cvdnet_consolidated.trigger_last_updated();

CREATE TRIGGER last_updated_measurements
    BEFORE UPDATE
    ON cvdnet_consolidated.measurements
    FOR EACH ROW
    EXECUTE PROCEDURE cvdnet_consolidated.trigger_last_updated();
