import pandas as pd
import sqlalchemy
import datasets
import random
import string

class SubjectDatabaseException(Exception):
    "Database inconsistency: subjects table"

# Check subject (raw name) exists
def check_subject_exists(subject_identifier, dataset_name, engine):
    """
    Check if a subject_identifier exists in the subjects table (also using the dataset_name)

    Parameters
    ----------
    subject_identifier: str
        The subject_identifier to search the subjects table
    dataset_name: str
        The name of the dataset that the subject is part of
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    bool
        Returns True if already exists, and False if it does not exist
    """    
    if datasets.check_dataset_name_exists(dataset_name=dataset_name,
                                          engine=engine) != True:
        raise SubjectDatabaseException("Dataset name \'{0}\' does not exist in the database.".format(dataset_name))

    result = pd.read_sql(f'SELECT count(*) ' 
                         f'FROM cvdnet_consolidated.subjects as s '
                         f'LEFT JOIN cvdnet_consolidated.datasets as d on d.id = s.dataset_id '
                         f'WHERE subject_identifier = \'{subject_identifier}\' '
                         f'AND dataset_name = \'{dataset_name}\';',
                         con=engine).loc[0, 'count']
    
    if result == 0:
        return False
    elif result == 1:
        return True
    elif result > 1:
        raise SubjectDatabaseException("More than one of this subject_identifer \'{0}\' exists in the database".format(subject_identifier))

# Check subject (deid name) exists
def check_subject_deid_exists(subject_identifier_deid, engine):
    """
    Check if a subject_identifier_deid exists in the subjects table

    Parameters
    ----------
    subject_identifier_deid: str
        The subject_identifier_deid to search the subjects table
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    bool
        Returns True if already exists, and False if it does not exist
    """    
    result = pd.read_sql(f'SELECT count(*) ' 
                         f'FROM cvdnet_consolidated.subjects as s '
                         f'WHERE subject_identifier_deid = \'{subject_identifier_deid}\';',
                         con=engine).loc[0, 'count']
    
    if result == 0:
        return False
    elif result == 1:
        return True
    elif result > 1:
        raise SubjectDatabaseException("More than one of this subject_identifer_deid \'{0}\' exists in the database".format(subject_identifier_deid))

# Get subject id (from raw name)
def get_subject_id(subject_identifier, dataset_name, engine):
    """
    Returns a id from the subject table for a given subject_identifier and dataset_name

    Parameters
    ----------
    subject_identifier: str
        The subject_identifier to search the subjects table for
    dataset_name: str
        The name of the dataset that the subject is part of
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    int
        The corresponding ID for the subject_identifier in the subjects table
    """
    if datasets.check_dataset_name_exists(dataset_name=dataset_name,
                                          engine=engine) != True:
        raise SubjectDatabaseException("Dataset name \'{0}\' does not exist in the database.".format(dataset_name))

    result = pd.read_sql(f'SELECT s.id ' 
                         f'FROM cvdnet_consolidated.subjects as s '
                         f'LEFT JOIN cvdnet_consolidated.datasets as d on d.id = s.dataset_id '
                         f'WHERE subject_identifier = \'{subject_identifier}\' '
                         f'AND dataset_name = \'{dataset_name}\';',
                         con=engine)
    
    if len(result.id) == 0:
        raise SubjectDatabaseException("No rows matching subject_identifier \'{0}\' found in the subjects table".format(subject_identifier))
    elif len(result.id) > 1:
        raise SubjectDatabaseException("Too many rows matching subject_identifier \'{0}\' found in the subjects table".format(subject_identifier))
    else:    
        return int(result.loc[0, 'id'])

# Get subject id (from deid name)
def get_subject_deid_id(subject_identifier_deid, engine):
    """
    Returns a id from the subject table for a given subject_identifier_deid

    Parameters
    ----------
    subject_identifier_deid: str
        The subject_identifier_deid to search the subjects table for
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    int
        The corresponding ID for the subject_identifier_deid in the subjects table
    """    
    result = pd.read_sql(f'SELECT id ' 
                         f'FROM cvdnet_consolidated.subjects as a '
                         f'WHERE subject_identifier_deid = \'{subject_identifier_deid}\';',
                         con=engine)
    
    if len(result.id) == 0:
        raise SubjectDatabaseException("No rows matching subject_identifier_deid \'{0}\' found in the subjects table".format(subject_identifier_deid))
    elif len(result.id) > 1:
        raise SubjectDatabaseException("Too many rows matching subject_identifier_deid \'{0}\' found in the subjects table".format(subject_identifier_deid))
    else:    
        return int(result.loc[0, 'id'])

# generate new unique random identifier
def generate_subject_identifier_deid(engine): 
    """
    Generates a new unique subject_identifier_deid that does not already exist in the subjects database

    Parameters
    ----------
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    str
        A new unique subject_identifier_deid. Note that this function does not insert it into the database nor reserve the identifier.
    """
    new_sub_deid = "".join(random.choices(string.ascii_letters, k=10))

    while check_subject_deid_exists(subject_identifier_deid=new_sub_deid,
                                    engine=engine) == True:
        new_sub_deid = "".join(random.choices(string.ascii_letters, k=10))
    
    return new_sub_deid

# insert subjects from a df
def insert_subjects(subjects_df, engine):
    """
    Using a dataframe of the key subject fields, inserts multiple new subjects into the subjects database table.

    Parameters
    ----------
    subjects_df: dataframe
        Dataframe of new subjects to insert with the following column structure: dataset_name, subject_identifier, gender ('M' or 'F'), date_of_birth ('YYYY-MM-DD'), date_of_death ('YYYY-MM-DD'), ethnicity
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    None
    """

    # Validate the structure of the dataframe against the template
    subject_template = pd.read_csv('templates/subjects_template.csv')
    if any(subjects_df.columns != subject_template.columns):
        raise SubjectDatabaseException("Columns of subject_df does not match template columns")

    # Check for duplicated rows
    if any(subjects_df.duplicated()) == True:
        raise SubjectDatabaseException("Duplicated rows in subject_df")

    # Check that dataset_name, subject_identifier are populated for each row
    to_check = ['dataset_name', 'subject_identifier']
    for var in to_check:
        if any(subjects_df[var].isna()):
            raise SubjectDatabaseException("{} contains NAs".format(var))

    # Check for blank strings in any cell
    if any(subjects_df.eq('').any(axis=1)):
        raise SubjectDatabaseException("Blank strings in dataframe")

    # Check that only 1 dataset name is present in the file
    if subjects_df['dataset_name'].nunique() != 1:
        raise SubjectDatabaseException("There should only be one dataset_name for all rows in subject_df")

    # Check that dataset_name already exists in the dataset table
    dataset_name = subjects_df['dataset_name'].unique().tolist()[0]
    if datasets.check_dataset_name_exists(dataset_name=dataset_name,
                                       engine=engine) == False:
        raise SubjectDatabaseException("dataset_name does not exist in the datasets database table")

    # Check that none of the subjects already exist in the database
    for sub in subjects_df['subject_identifier']:
        if check_subject_exists(subject_identifier=sub,
                                dataset_name=dataset_name,
                                engine=engine) == True:
            raise SubjectDatabaseException("Subject {0} already exists in the subjects table".format(sub))

    # Check that gender is from ['F', 'M']
    if any(gen not in ['F', 'M'] for gen in subjects_df[subjects_df['gender'].notna()]['gender']):
        raise SubjectDatabaseException("gender should be either 'F' or 'M'")

    # Transform columns to date
    subjects_df['date_of_birth'] = pd.to_datetime(subjects_df['date_of_birth'])
    subjects_df['date_of_death'] = pd.to_datetime(subjects_df['date_of_death'])

    # Check that DOB < DOD
    if any(subjects_df['date_of_birth'] > subjects_df['date_of_death']):
        raise SubjectDatabaseException("DOB is later than DOD for at least 1 subject")

    # Generate de-identified IDs for every subject
    subjects_df['subject_identifier_deid'] = None
    for index, row in subjects_df.iterrows():
        de_id = generate_subject_identifier_deid(engine=engine)
        while de_id in subjects_df['subject_identifier_deid'].unique():
            de_id = generate_subject_identifier_deid(engine=engine)

        subjects_df.loc[index, 'subject_identifier_deid'] = de_id

    # Prepare insertion dataframe
    insert_subs = pd.read_sql(f'SELECT * '
                              f'FROM cvdnet_consolidated.subjects '
                              f'LIMIT 0;',
                              con=engine).drop(['id', 'date_last_updated'], axis=1)

    insert_subs['subject_identifier'] = subjects_df['subject_identifier']
    insert_subs['dataset_id'] = datasets.get_dataset_id(dataset_name=dataset_name,
                                                        engine=engine)
    insert_subs['subject_identifier_deid'] = subjects_df['subject_identifier_deid']
    insert_subs['gender'] = subjects_df['gender']
    insert_subs['date_of_birth'] = subjects_df['date_of_birth']
    insert_subs['date_of_death'] = subjects_df['date_of_death']
    insert_subs['ethnicity'] = subjects_df['ethnicity']

    insert_subs.to_sql(name="subjects",
                       con=engine,
                       if_exists='append',
                       schema='cvdnet_consolidated',
                       index=False)