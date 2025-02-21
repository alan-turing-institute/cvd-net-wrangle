import pandas as pd
import re
import datasets
import subjects
import metadata

class MeasurementFileException(Exception):
    pass

class FormattedMeasurements:
    """Class for measurement dataframes that have been correctly formatted. Stops unformatted dataframes being used"""
    def __init__(self, df):
        self.df = df
        self.formatted = True

# Load measurement file
def load_measurement_file(path_to_file):
    """
    Loads pre-formatted measurement data ready to be inserted into the database. Checks the file is valid.

    Parameters
    ----------
    path_to_file: str
        The path to the input csv file of pre-transformed (to template format) measurements

    Returns
    -------
    FormattedMeasurements object
        Object of class FormattedMeasurements containing the formatted dataframe
    """
    # All data loaded in as strings
    # Datatype is modified later on when required
    # Loading in columns as native datatype caused some issues (e.g. missing leading zeros from identifiers)
    meas = pd.read_csv(filepath_or_buffer=path_to_file,
                       skipinitialspace=True,
                       dtype=str)
    
    template = pd.read_csv("templates/data_template.csv")

    # QC checks on measurement file format and content
    # Whole file
    if any(meas.columns != template.columns):
        raise MeasurementFileException("The columns in the measurement file do not match those of the template")
    if any(meas.duplicated()) == True:
        raise MeasurementFileException("The measurement file contains duplicated rows")
    if any(meas.eq('').any(axis=0)) == True:
        raise MeasurementFileException("Blank strings found in measurement file  - these should be NA")
    
    # Check that certain fields are populated in each row
    to_check = ['dataset_name',
                'subject_identifier',
                'variable_name']
    for var in to_check:
        if any(meas[var].isna()):
            raise MeasurementFileException("{} contains NAs".format(var))

    # Dataset_name
    if meas['dataset_name'].nunique() != 1:
        raise MeasurementFileException("Only one dataset_name should be in a dictionary file")
    if ' ' in meas['dataset_name']:
        raise MeasurementFileException("Dataset name contains whitespace")
    
    # Gender
    if any(gen not in ['F', 'M'] for gen in meas[meas['gender'].notna()]['gender']):
        raise MeasurementFileException("gender should be either 'F' or 'M'")
    
    # Check dates are formatted as YYYY-MM-DD
    date_fields = ['date_of_birth',
                   'date_of_death',
                   'measurement_date']
    for field in date_fields:
        if any(meas[field].str.match("^[12][890][0-9]{2}-[0-1][0-9]-[0-3][0-9]$") == False):
            raise MeasurementFileException("At least 1 value in \'{0}\' not formatted as YYYY-MM-DD with possible values".format(field))

    # Check times formatted as HH:MM:SS (seconds can be decimal) (24HR clock)
    time_fields = ['measurement_time']
    for field in time_fields:
        if any(meas[field].str.match("^([0-1][0-9]|2[0-4]):[0-5][0-9]:[0-5][0-9][.]{0,1}[0-9]{0,}$") == False):
            raise MeasurementFileException("At least 1 value in \'{0}\' not formatted as HH-MM-SS with possible values".format(field))

    formatted_meas = FormattedMeasurements(meas)

    return formatted_meas
    
# Check measurement exists (for dataset/subject/measurement_date/measurement_time/visit_grouping/variable combinations)
def check_measurement_exists(dataset_name,
                             subject_identifier,
                             variable_name,
                             engine,
                             measurement_date=None,
                             measurement_time=None,
                             visit_grouping=None):
    """
    Check if a measurement exists in the measurement table (using dataset_name, subject_identifier, variable_name; plus optional fields: measurement_date, measurement_time, visit_grouping).
    Note that this function will not check whether the dataset_name, subject_identifier, or variable_name exist in the database - this should be verified alternatively.

    Parameters
    ----------
    dataset_name: str
        The name of the dataset that the subject is part of
    subject_identifier: str
        The raw subject_identifier for the measurement
    variable_name: str
        The name of the variable (either with/without the dataset_name appended to the start)
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database
    measurement_date: str
        The date of the measurement (optional)
    measurement_time: str
        The time of the measurement (optional)
    visit_grouping: str
        The visit grouping (optional)

    Returns
    -------
    bool
        Returns True if already exists, and False if it does not exist
    """
    # If variable name does not have the dataset_name appended at the start then amend that
    if bool(re.match(f'^{dataset_name}_', variable_name)) == False:
        variable_name = f'{dataset_name}_{variable_name}'

    # Generate strings depending on whether the measurement date/time and visit_grouping are submitted
    if pd.isna(measurement_date) == False:
        m_d_string = f'AND measurement_date = \'{measurement_date}\' '
    else:
        m_d_string = f''

    if pd.isna(measurement_time) == False:
        m_t_string = f'AND measurement_time = \'{measurement_time}\' '
    else:
        m_t_string = f''

    if pd.isna(visit_grouping) == False:
        v_g_string = f'AND visit_grouping = \'{visit_grouping}\' '
    else:
        v_g_string = f''

    # Generate a query from the strings
    query = ('SELECT count(*) ' + 
             'FROM cvdnet_consolidated.measurements as m ' + 
             'LEFT JOIN cvdnet_consolidated.subjects as s on s.id = m.subject_id ' +
             f'LEFT JOIN cvdnet_consolidated.datasets as d on d.id = s.dataset_id ' +
             f'LEFT JOIN cvdnet_consolidated.metadata_variables as v on v.id = m.variable_id ' +
             f'WHERE subject_identifier = \'{subject_identifier}\' ' +
             f'AND dataset_name = \'{dataset_name}\' ' +
             f'AND variable_name = \'{variable_name}\' ' +
             m_d_string + 
             m_t_string +
             v_g_string +
             ';')
    
    result = pd.read_sql(sql=query,
                         con=engine).loc[0, 'count']

    if result == 0:
        return False
    elif result == 1:
        return True
    elif result > 1:
        raise MeasurementFileException("More than one of this measurement exists in the database")


# Get measurement ID
# TODO: add function

# Insert measurements into the database
# Using the data pre-transformed into the template format
# This function will also insert new subjects
def insert_measurements(formatted_meas, engine):
    """
    Inserts measurements into the measurements table of the database, as well as new subjects

    Parameters
    ----------
    formatted_meas: object
        An object of class FormattedMeasures (from load_measurement_file function)
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database
    
    Returns
    -------
    none
    """
    
    if isinstance(formatted_meas, FormattedMeasurements) == False:
        raise MeasurementFileException("formatted_meas is not an instance of FormattedMeasurements class")
    
    if formatted_meas.formatted == False:
        raise MeasurementFileException("Dataframe in formatted_meas is not set as formatted")

    print("Inserting new measurements into the database\n")
    print("Insertions will happen to the following tables in the following order (if required):")
    print("1) subjects, 2) measurements\n")

    # Prepare to insert new subjects
    # Start with looking up datasets
    dataset_name = formatted_meas.df['dataset_name'].unique().tolist()[0]
    if datasets.check_dataset_name_exists(dataset_name=dataset_name, 
                                          engine=engine) == False:
        MeasurementFileException("dataset_name \'{0}\' does not exist in the datasets table".format(dataset_name))

    dataset_id = datasets.get_dataset_id(dataset_name, engine)

    # df of unique subjects
    # Then check whether they already exist in the database
    uniq_subs = formatted_meas.df[['dataset_name',
                                   'subject_identifier',
                                   'gender',
                                   'date_of_birth',
                                   'date_of_death',
                                   'ethnicity']].drop_duplicates().reset_index(drop=True)
    uniq_subs['exists'] = None
    uniq_subs['matches'] = None
    for index, row in uniq_subs.iterrows():
        if subjects.check_subject_exists(subject_identifier=uniq_subs.loc[index, 'subject_identifier'],
                                         dataset_name=uniq_subs.loc[index, 'dataset_name'],
                                         engine=engine) == True:
            uniq_subs.loc[index, 'exists'] = True
        else:
            uniq_subs.loc[index, 'exists'] = False

    # Drop subjects that already exist
    old_subs = len(uniq_subs[uniq_subs['exists'] == True])
    subs_to_insert = uniq_subs[uniq_subs['exists'] == False]
    subs_to_insert = subs_to_insert.drop(['exists', 'matches'], axis=1)
    subs_to_insert = subs_to_insert.convert_dtypes()
    new_subs = len(subs_to_insert)

    # Insert the new subs into the subjects table
    if new_subs > 0:
        print("Inserting {0} new subjects. {1} subjects already existed".format(new_subs,
                                                                                old_subs))
        subjects.insert_subjects(subjects_df=subs_to_insert,
                                 engine=engine)
    else:
        print("No new subjects to insert. {0} subjects already exist in the subjects table".format(old_subs))
        
    # Now check each subject exists and if so, get subject id for each unqiue subject 
    print("Preparing to insert measurements...")
    print("Checking subjects...")
    uniq_subs['subject_id'] = None
    for index, row in uniq_subs.iterrows():
        if uniq_subs.loc[index, 'exists'] == False:
            if subjects.check_subject_exists(subject_identifier=uniq_subs.loc[index, 'subject_identifier'],
                                             dataset_name=uniq_subs.loc[index, 'dataset_name'],
                                             engine=engine) == True:
                uniq_subs.loc[index, 'exists'] = True
            else:
                raise MeasurementFileException("subject \'{0}\' does not exist in the subjects table".format(uniq_subs.loc[index, 'subject_identifier']))
        
        uniq_subs.loc[index, 'subject_id'] = subjects.get_subject_id(subject_identifier=uniq_subs.loc[index, 'subject_identifier'],
                                                                     dataset_name=uniq_subs.loc[index, 'dataset_name'],
                                                                     engine=engine)

    # Make a df of all the unique variables
    print("Checking variables...")
    uniq_vars = formatted_meas.df[['dataset_name',
                                   'variable_name']].drop_duplicates().reset_index(drop=True)
    uniq_vars['variable_name_formatted'] = uniq_vars['dataset_name'] + '_' + uniq_vars['variable_name']

    # Check whether each variable exists in the database
    uniq_vars['exists'] = None
    uniq_vars['variable_id'] = None
    for index, row in uniq_vars.iterrows():
        uniq_vars.loc[index, 'exists'] = metadata.check_variable_name_exists(name=uniq_vars.loc[index, 'variable_name_formatted'],
                                                                              engine=engine)
        if uniq_vars.loc[index, 'exists'] == False:
            raise MeasurementFileException("variable_name \'{0}\' does not exist in the variables table". format(uniq_vars.loc[index, 'variable_name_formatted']))
        else:
            uniq_vars.loc[index, 'variable_id'] = metadata.get_variable_id(variable_name=uniq_vars.loc[index, 'variable_name_formatted'],
                                                                            engine=engine)
        
    # Pull the variables information from the database for all of the variables in uniq_vars
    id_list = ",".join(map(str, uniq_vars['variable_id']))
    variables_db = pd.read_sql(f'SELECT v.id as variable_id, variable_name, data_type, associated_visit, has_options, range_min, range_max, deidentification_required, option_name '
                               f'FROM cvdnet_consolidated.metadata_variables as v '
                               f'LEFT JOIN cvdnet_consolidated.metadata_variable_options as vo on vo.variable_id = v.id '
                               f'WHERE v.id in ({id_list});',
                               con=engine)

    # Go through every row in formatted_meas.df and check that the value is valid against the details of the variable in the database
    print("Validating measurements to be inserted (this may take a while)...")
    formatted_meas.df['subject_id'] = None
    formatted_meas.df['variable_id'] = None
    formatted_meas.df['value_deid'] = None
    formatted_meas.df['meas_exists'] = False
    for index, row in formatted_meas.df.iterrows():
        # Retrieve some key info for easier acces
        formatted_var_name = formatted_meas.df.loc[index, 'dataset_name'] + '_' + formatted_meas.df.loc[index, 'variable_name']
        variable_details = variables_db[variables_db['variable_name'] == formatted_var_name]
        data_type = variables_db.loc[variables_db['variable_name'] == formatted_var_name, 'data_type'].values[0]

        value = formatted_meas.df.loc[index, 'value']
        subject_identifier = formatted_meas.df.loc[index, 'subject_identifier']

        # Populate the subject and variable ID fields - looked up in other dataframes
        formatted_meas.df.loc[index, 'subject_id'] = uniq_subs.loc[uniq_subs['subject_identifier'] == subject_identifier, 'subject_id'].to_list()[0]
        formatted_meas.df.loc[index, 'variable_id'] = variables_db.loc[variables_db['variable_name'] == formatted_var_name, 'variable_id'].to_list()[0]

        # No need to validate values when the value to be inserted is NULL 
        if pd.isna(value) == False:
            # First validate the values against the defined data types for each variable
            if data_type == 'float':
                try:
                    float(value)
                except:
                    raise MeasurementFileException("value \'{0}\' for variable_name \'{1}\' cannot be converted to float".format(value, formatted_var_name))
                
            elif data_type == 'int':
                try:
                    int(value)
                except:
                    raise MeasurementFileException("value \'{0}\' for variable_name \'{1}\' cannot be converted to int".format(value, formatted_var_name))
                
            elif data_type == 'boolean':
                try:
                    bool(value)
                except:
                    raise MeasurementFileException("value \'{0}\' for variable_name \'{1}\' cannot be converted to boolean".format(value, formatted_var_name))
                
            elif data_type == 'date':
                if bool(re.match("^[12][890][0-9]{2}-[0-1][0-9]-[0-3][0-9]$", value)) == False:
                    raise MeasurementFileException("value \'{0}\' for variable_name \'{1}\' cannot be converted to date".format(value, formatted_var_name))
                                
            elif data_type == 'time':
                if bool(re.match("^([0-1][0-9]|2[0-4]):[0-5][0-9]:[0-5][0-9][.]{0,1}[0-9]{0,}$", value)) == False:
                    raise MeasurementFileException("value \'{0}\' for variable_name \'{1}\' cannot be converted to time".format(value, formatted_var_name)) 

            # If a variable has options, then validate the value against them
            if variable_details['has_options'].values[0] == True:
                if value not in variable_details['option_name'].values:
                    raise MeasurementFileException("Value \'{0}\' is not valid option for variable \'{1}\'".format(value, formatted_var_name))
                
            # If a variable does not require de-identification then simply copy the raw value to the 'value_deid' column
            # Apply deidentification when required or leave as NULL if no method available
            if variable_details['deidentification_required'].values[0] == True:
                # TODO: apply the deidentification here
                # Use variable_details['deidentification_method'] column to extact the method
                # These methods will need populating in the metadata tables when loading the data dictionaries into them
                    # Using controlled vocabulary where possible. Examples:
                        # 'REMOVE' for when a value should be completey removed (-> insert null into 'value_deid')
                        # 'ANON_DATE' for when the month and day of a date should be deidentified (-> set day and month to '01'))
                # Implement code to make these changes HERE!
                pass
            else:
                formatted_meas.df.loc[index, 'value_deid'] = value

        # If measurement_date present check that is formatted correctly and is > DOB and < DOD
        measurement_date = formatted_meas.df.loc[index, 'measurement_date']
        if pd.isna(measurement_date) == False:
            if bool(re.match("^[12][890][0-9]{2}-[0-1][0-9]-[0-3][0-9]$", measurement_date)) == False:
                raise MeasurementFileException("measurement_date \'{0}\' not correctly formatted for a date".format(measurement_date))
            
            if pd.to_datetime(measurement_date) < pd.to_datetime(formatted_meas.df.loc[index, 'date_of_birth']):
                raise MeasurementFileException("measurement_date \'{0}\' is earlier than subject's date_of_birth \'{1}\'".format(measurement_date,
                                                                                                                       formatted_meas.df.loc[index, 'date_of_birth']))

            if pd.isna(formatted_meas.df.loc[index, 'date_of_death']) == False:
                if pd.to_datetime(measurement_date) > pd.to_datetime(formatted_meas.df.loc[index, 'date_of_death']):
                    raise MeasurementFileException("measurement_date \'{0}\' is later than subject's date_of_death \'{1}\'".format(measurement_date,
                                                                                                                       formatted_meas.df.loc[index, 'date_of_death']))
                
        # If measurement_time populated then check it is formatted correctly
        measurement_time = formatted_meas.df.loc[index, 'measurement_time']
        if pd.isna(measurement_time) == False:
            if bool(re.match("^([0-1][0-9]|2[0-4]):[0-5][0-9]:[0-5][0-9][.]{0,1}[0-9]{0,}$", measurement_time)) == False:
                raise MeasurementFileException("measurement_time \'{0}\' not formatted correctly for a time".format(measurement_time)) 

        # If visit_grouping is NULL and a visit grouping is found in the metadata -> pull that visit_grouping
        if pd.isna(formatted_meas.df.loc[index, 'visit_grouping']) == True and pd.isna(variable_details['associated_visit'].values[0]) == False:
            formatted_meas.df.loc[index, 'visit_grouping'] = variable_details['associated_visit'].values[0]

        # Check whether a value exists for a dataset/sub/variable/date/time/visit combination
        if check_measurement_exists(dataset_name=dataset_name,
                                    subject_identifier=subject_identifier,
                                    variable_name=formatted_var_name,
                                    engine=engine,
                                    measurement_date=measurement_date,
                                    measurement_time=measurement_time,
                                    visit_grouping=formatted_meas.df.loc[index, 'visit_grouping']) == True:
            formatted_meas.df.loc[index, 'meas_exists'] = True

    # Prepare the data for insertion
    # Drop measurement that are already present
    old_meas = len(formatted_meas.df[formatted_meas.df['meas_exists'] == True])
    formatted_meas.df = formatted_meas.df[formatted_meas.df['meas_exists'] == False]
    formatted_meas.df = formatted_meas.df.convert_dtypes()
    new_meas = len(formatted_meas.df)

    # Insert the new measurements into the measurements table
    if new_meas > 0:
        print("Inserting {0} new measurements. {1} measurements already existed".format(new_meas, 
                                                                                        old_meas))
        insert_meas = pd.read_sql(f'SELECT * '
                                f'FROM cvdnet_consolidated.measurements '
                                f'LIMIT 0;',
                                con=engine).drop(['id', 'date_last_updated'], axis=1)

        insert_meas['subject_id'] = formatted_meas.df['subject_id']
        insert_meas['variable_id'] = formatted_meas.df['variable_id']
        insert_meas['measurement_date'] = formatted_meas.df['measurement_date']
        insert_meas['measurement_time'] = formatted_meas.df['measurement_time']
        insert_meas['visit_grouping'] = formatted_meas.df['visit_grouping']
        insert_meas['value'] = formatted_meas.df['value']
        insert_meas['value_deid'] = formatted_meas.df['value_deid']

        insert_meas.to_sql(name="measurements",
                        con=engine,
                        if_exists='append',
                        schema='cvdnet_consolidated',
                        index=False)
    else:
        print("No new measurements to insert. {0} measurements already exist in the measurements table".format(old_meas))
    