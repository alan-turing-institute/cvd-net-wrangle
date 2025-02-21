import pandas as pd
import sqlalchemy
import database_interaction
import datasets
import annotations

class DictionaryException(Exception):
    pass

class FormattedDictionary:
    """Class for dictionary dataframes that have been correctly formatted. Stops unformatted dataframes being used"""
    def __init__(self, df):
        self.df = df
        self.formatted = True

def load_dictionary_file(path_to_file):
    """
    Loads pre-formatted dictionary data ready to be inserted into the database. Checks the file is valid.

    Parameters
    ----------
    path_to_file: str
        The path to the input csv file of pre-transformed (to template format) dictionary

    Returns
    -------
    FormattedDictionary object
        Object of class FormattedDictionary containing the formatted dataframe
    """
    dict = pd.read_csv(path_to_file, 
                       dtype={'range_min': 'float',
                              'range_max': 'float',
                              'option_name': 'object'},
                              skipinitialspace=True)

    template = pd.read_csv('templates/dictionary_template.csv')
    
    # QC checks on file formatting and content
    # Whole file
    if any(dict.columns != template.columns):
        raise DictionaryException("The format of the input file does not match the template")
    if any(dict.duplicated()):
        raise DictionaryException("One or more rows are duplicated in the input file")
    if any(dict.eq('').any(axis=0)):
        raise DictionaryException("Blank strings present in file - these should be NA")

    # dataset_name
    if any(dict['dataset_name'].isna()):
        raise DictionaryException("dataset_name is not populated in every row")
    if dict['dataset_name'].nunique() != 1:
        raise DictionaryException("Only one dataset_name should be in a dictionary file")
    if ' ' in dict['dataset_name']:
        raise DictionaryException("Dataset name contains whitespace")
    
    # variable_name
    if any(dict['variable_name'].isna()):
        raise DictionaryException("variable_name should be populated in every row")

    # data_type
    if any(dict['data_type'].isna()):
        raise DictionaryException("data_type should be populated in every row")
    allowed_data_types = ['str',
                          'int',
                          'date',
                          'boolean',
                          'float']
    if any(dt not in allowed_data_types for dt in dict['data_type'].unique()):
        raise DictionaryException("Unallowed datatypes in data_type column")

    # category_levels
    if any(dict['category_level_2'].notna() & dict['category_level_1'].isna()):
        raise DictionaryException("category_level_2 cannot be populated if category_level_1 is not")

    # options
    if any(dict['has_options'].isna()):
        raise DictionaryException("has_options should be populated in every row")
    if any(opt not in [0, 1] for opt in dict['has_options']):
        raise DictionaryException("has_options should be in [0, 1] for every row")
    if any(dict['option_name'].notna() & dict['option_description'].isna()):
        raise DictionaryException("option_description cannot be empty when option_name is present")
    if any(dict['has_options'] == 1 & dict['option_name'].isna()):
        raise DictionaryException("option_name is empty when has_options = 1")
    vars_with_options = dict[dict['has_options'] == 1]['variable_name'].unique()
    for var in vars_with_options:
        n_rows = len(dict[dict['variable_name'] == var])
        n_unique_options = dict[dict['variable_name'] == var]['option_name'].nunique()
        if n_rows != n_unique_options:
            raise DictionaryException("Non unique option_names submitted for the same variable_name")

    # ranges
    if any(dict['range_min'].notna() & 
           pd.Series([dt not in ['int', 'float'] for dt in dict['data_type']])):
        raise DictionaryException("range_min populated for non-numeric variable(s)")
    if any(dict['range_max'].notna() & 
           pd.Series([dt not in ['int', 'float'] for dt in dict['data_type']])):
        raise DictionaryException("range_max populated for non-numeric variable(s)")

    # deidentification_required
    if any(dict['deidentification_required'].isna()):
        raise DictionaryException("deidentification_required should be populated in every row")
    if any(opt not in [0, 1] for opt in dict['deidentification_required']):
        raise DictionaryException("deidentification_required should be in [0, 1] for every row")

    # deidentification_method
    # TODO: add checks here in future (e.g. if deidentification_method populated then deidentification_required == 1)

    # variable_source
    if any(dict['variable_source'].isna()):
        raise DictionaryException("variable_source should be populated in every row")
    if any(vs not in ['ORIGINAL', 'DERIVED'] for vs in dict['variable_source']):
        raise DictionaryException("deidentification_required should be in ['Original', 'Deried'] for every row")

    # Categories capitalised
    dict['category_level_1'] = dict['category_level_1'].str.strip().str.upper()
    dict['category_level_2'] = dict['category_level_2'].str.strip().str.upper()
    dict['has_options'] = dict['has_options'].astype('bool')
    dict['deidentification_required'] = dict['deidentification_required'].astype('bool')

    formatted_df = FormattedDictionary(dict)

    return formatted_df

class VariablesDatabaseException(Exception):
    "Database inconsistency: variable table"

def check_variable_name_exists(name, engine):
    """
    Check if a variable name exists in the metadata_variable table

    Parameters
    ----------
    name: str
        The variable name to search the variables table
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    bool
        Returns True if already exists, and False if it does not exist
    """    
    result = pd.read_sql(f'SELECT count(*) ' 
                         f'FROM cvdnet_consolidated.metadata_variables '
                         f'WHERE variable_name = \'{name}\';',
                         con=engine).loc[0, 'count']
    
    if result == 0:
        return False
    elif result == 1:
        return True
    elif result > 1:
        raise VariablesDatabaseException("More than one of this variable_name exists in the database")
    
def get_variable_id(variable_name, engine):
    """
    Returns a id from the metadata_variables table for a given variable_name

    Parameters
    ----------
    variable_name: str
        The variable_name to search the variables table
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    int
        The corresponding ID for the variable_name in the metadata_variables table
    """
    result = pd.read_sql(f'SELECT id ' 
                         f'FROM cvdnet_consolidated.metadata_variables '
                         f'WHERE variable_name = \'{variable_name}\';',
                         con=engine)
    
    if len(result.id) == 0:
        raise VariablesDatabaseException("No rows matching specified variable_name in the metadata_variables table")
    elif len(result.id) > 1:
        raise VariablesDatabaseException("Too many rows matching specified variable_name in the metadata_variables table")
    else:    
        return int(result.loc[0, 'id'])

def check_variable_option_name_exists(option_name, variable_id, engine):
    """
    Check if a variable_option name exists in the metadata_variable_options table for a given variable

    Parameters
    ----------
    option_name: str
        The variable_option name to search the metadata_variable_options table
    variable_id: int
        The variable_id to search for
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    bool
        Returns True if already exists, and False if it does not exist
    """
    result = pd.read_sql(f'SELECT count(*) ' 
                         f'FROM cvdnet_consolidated.metadata_variable_options '
                         f'WHERE option_name = \'{option_name}\' and variable_id = {variable_id};',
                         con=engine).loc[0, 'count']
    
    if result == 0:
        return False
    elif result == 1:
        return True
    elif result > 1:
        raise VariablesDatabaseException("More than one of this option_name exists in the database for the variable_id \'{0}\'".format(variable_id))

def get_variable_option_id(option_name, variable_id, engine):
    """
    Returns a id from the metadata_variable_options table for a given option_name and variable_id

    Parameters
    ----------
    option_name: str
        The variable_option name to search the metadata_variable_options table
    variable_id: int
        The variable_id to search for
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    int
        The corresponding ID for the variable_name in the metadata_variables table
    """
    result = pd.read_sql(f'SELECT id ' 
                         f'FROM cvdnet_consolidated.metadata_variable_options '
                         f'WHERE option_name = \'{option_name}\' and variable_id = {variable_id};',
                         con=engine)
    
    if len(result.id) == 0:
        raise VariablesDatabaseException("No rows matching specified options_name and variable_id in the metadata_variable_options table")
    elif len(result.id) > 1:
        raise VariablesDatabaseException("Too many rows matching specified option_name and variable_id in the metadata_variable_options table")
    else:    
        return result.loc[0, 'id']

def insert_variables(formatted_dictionary, engine):
    """
    Inserts variables into the metadata_variables table of the database, as well as relevant datasets, annotations, and metadata_variable_options

    Parameters
    ----------
    formatted_dictionary: object
        An object of class FormattedDictionary (from load_dictionary_file function)
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database
    
    Returns
    -------
    none
    """
    if isinstance(formatted_dictionary, FormattedDictionary) == False:
        raise VariablesDatabaseException("formatted_dictionary is not an instance of FormattedDictionary class")
    
    if formatted_dictionary.formatted == False:
        raise VariablesDatabaseException("Dataframe in formatted_dictionary is not set as formatted")

    print("Inserting new metadata variables into the database\n")
    print("Insertions will happen to the following tables in the following order (as required):")
    print("1) datasets, 2) annotations, 3) metadata_variables, 4) metadata_variable_options\n")

    # Insert the dataset to the database if new
    dataset_name = formatted_dictionary.df['dataset_name'].unique().tolist()[0]
    if datasets.check_dataset_name_exists(dataset_name, engine) == False:
        user_dataset_insert = database_interaction.user_choice('Insert new dataset \'{0}\' into datasets table?'.format(dataset_name))
        if user_dataset_insert == True:            
            datasets.insert_dataset(dataset_name, engine)
            print("Inserted new dataset: \'{0}\'".format(dataset_name))
        else:
            raise VariablesDatabaseException("User asked not to insert dataset - stopping insert process")
    else:
        print("No new dataset to insert. \'{0}\' already exists".format(dataset_name))
    dataset_id = datasets.get_dataset_id(dataset_name, engine)
    
    # Prepare a dataframe of annotations to insert if they are new
    annotations_df = formatted_dictionary.df[['category_level_1', 'category_level_2']]
    annotations_df = annotations_df.drop_duplicates().reset_index(drop=True)
    annotations_df['exists'] = None
    for index, row in annotations_df.iterrows():
        annotations_df.loc[index, 'exists'] = annotations.check_category_exists(annotations_df.loc[index, 'category_level_1'],
                                                                              annotations_df.loc[index, 'category_level_2'],
                                                                              engine)
    old_cats = len(annotations_df[annotations_df['exists'] == True])
    annotations_df = annotations_df[annotations_df['exists'] == False].drop('exists', axis=1)

    # Insert the new annotations into the database
    new_cats = len(annotations_df)
    if new_cats > 0:
        user_annotations_insert = database_interaction.user_choice('{0} annotations already exist. Insert {1} new category combinations into annotations table?'.format(old_cats, new_cats))
        if user_annotations_insert == True:
            annotations.insert_annotations(annotations_df, engine)
            print("Inserted {0} new annotations".format(new_cats))
        else:
            raise VariablesDatabaseException("User asked not to insert new annotations - stopping insert process")
    else:
        print("No new annotations to insert. {0} annotations already exist".format(old_cats))

    # Get the category ID for each row in the formatted dictionary file
    formatted_dictionary.df['category_id'] = None
    for index, row in formatted_dictionary.df.iterrows():
        if pd.isna(row['category_level_1']) == False:
            formatted_dictionary.df.loc[index, 'category_id'] = annotations.get_annotation_id(formatted_dictionary.df.loc[index, 'category_level_1'], 
                                                                                              formatted_dictionary.df.loc[index, 'category_level_2'], 
                                                                                              engine)

    # Format the variable names
    formatted_dictionary.df['formatted_variable_name'] = dataset_name + '_' + formatted_dictionary.df['variable_name']

    # Check for any variables that already exist in the database
    formatted_dictionary.df['variable_exists'] = None
    formatted_dictionary.df['variable_exists'] =  formatted_dictionary.df['variable_exists'].astype(bool)
    for index, row in formatted_dictionary.df.iterrows():
        formatted_dictionary.df.loc[index, 'variable_exists'] = check_variable_name_exists(formatted_dictionary.df.loc[index, 'formatted_variable_name'], engine)
    vars_to_insert = ~formatted_dictionary.df['variable_exists']

    # Populate the dataframe to be inserted into the database variables table
    insert_vars = pd.read_sql(f'SELECT * '
                            f'FROM cvdnet_consolidated.metadata_variables '
                            f'LIMIT 0;',
                            con=engine)
    insert_vars = insert_vars.drop(['id', 'date_last_updated'], axis=1)

    insert_vars['variable_name'] = formatted_dictionary.df.loc[vars_to_insert, 'formatted_variable_name']
    insert_vars['dataset_id'] = dataset_id
    insert_vars['variable_description'] = formatted_dictionary.df.loc[vars_to_insert, 'variable_description']
    insert_vars['data_type'] = formatted_dictionary.df.loc[vars_to_insert, 'data_type']
    insert_vars['unit'] = formatted_dictionary.df.loc[vars_to_insert, 'unit']
    insert_vars['associated_visit'] = formatted_dictionary.df.loc[vars_to_insert, 'associated_visit']
    insert_vars['category_id'] = formatted_dictionary.df.loc[vars_to_insert, 'category_id']
    insert_vars['has_options'] = formatted_dictionary.df.loc[vars_to_insert, 'has_options']
    insert_vars['range_min'] = formatted_dictionary.df.loc[vars_to_insert, 'range_min']
    insert_vars['range_max'] = formatted_dictionary.df.loc[vars_to_insert, 'range_max']
    insert_vars['deidentification_required'] = formatted_dictionary.df.loc[vars_to_insert, 'deidentification_required']
    insert_vars['deidentification_method'] = formatted_dictionary.df.loc[vars_to_insert, 'deidentification_method']
    insert_vars['variable_source'] = formatted_dictionary.df.loc[vars_to_insert, 'variable_source']
    insert_vars = insert_vars.drop_duplicates()
    insert_vars = insert_vars.convert_dtypes() # For some reason psycopg2 has issues with int64 if you don't run this
    
    # Insert the new metadata_variables into the database
    new_vars = len(insert_vars)
    old_vars = len(formatted_dictionary.df[formatted_dictionary.df['variable_exists'] == True])

    if new_vars > 0:
        user_variables_insert = database_interaction.user_choice('{0} variables already exist in the metadata_variables table. Insert {1} new variables into the table?'.format(old_vars, 
                                                                                                                                                                   new_vars))
        if user_variables_insert == True:
            insert_vars.to_sql(name='metadata_variables',
                             con=engine,
                             if_exists='append',
                             schema='cvdnet_consolidated',
                             index=False)
            print("Inserted {0} new variables".format(new_vars))
        else:
            raise VariablesDatabaseException("User asked not to insert new variables - stopping insert process")
    else:
        print("No new variables to insert. {0} already exist".format(old_vars))

    # Now prepare the metadata_variable_options for insertion
    formatted_dictionary.df['variable_id'] = None
    formatted_dictionary.df['option_exists'] = None
    formatted_dictionary.df['option_exists'] = formatted_dictionary.df['option_exists'].astype(bool)

    has_options = formatted_dictionary.df['has_options']
    for index, row in formatted_dictionary.df[has_options].iterrows():
        formatted_dictionary.df.loc[index, 'variable_id'] = get_variable_id(row['formatted_variable_name'], engine)
        formatted_dictionary.df.loc[index, 'option_exists'] = check_variable_option_name_exists(formatted_dictionary.df.loc[index, 'option_name'],
                                                                                                formatted_dictionary.df.loc[index, 'variable_id'],
                                                                                                engine)
    opts_to_insert = has_options & ~formatted_dictionary.df['option_exists']
    opts_exist = formatted_dictionary.df['option_exists']

    # Create df for insertion to options table
    insert_opts = pd.read_sql(f'SELECT * '
                            f'FROM cvdnet_consolidated.metadata_variable_options '
                            f'LIMIT 0;',
                            con=engine)
    
    insert_opts = insert_opts.drop(['id', 'date_last_updated'], axis=1)
    insert_opts['variable_id'] = formatted_dictionary.df.loc[opts_to_insert, 'variable_id']
    insert_opts['option_name'] = formatted_dictionary.df.loc[opts_to_insert, 'option_name']
    insert_opts['option_description'] = formatted_dictionary.df.loc[opts_to_insert, 'option_description']
    insert_opts = insert_opts.convert_dtypes() # For some reason psycopg2 has issues with int64 if you don't run this

    # Insert options
    new_opts = len(insert_opts)
    old_opts = len(formatted_dictionary.df[opts_exist] == True)

    if new_opts > 0:
        user_options_insert = database_interaction.user_choice('{0} options already exist in the metadata_variable_options table. Insert {1} new variable_options into the table?'.format(old_opts, 
                                                                                                                                                           new_opts))
        if user_options_insert == True:
            insert_opts.to_sql(name='metadata_variable_options',
                             con=engine,
                             if_exists='append',
                             schema='cvdnet_consolidated',
                             index=False)
            print("Inserted {0} new variable_options".format(new_opts))
        else:
            raise VariablesDatabaseException("User asked not to insert new metadata_variable_options - stopping insert process")
    else:
        print("No new metadata_variable_options to insert. {0} already exist".format(old_opts))
