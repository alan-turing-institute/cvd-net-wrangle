import pandas as pd
import sqlalchemy

class DatasetDatabaseException(Exception):
    "Database inconsistency: dataset table"

def check_dataset_name_exists(dataset_name, engine):
    """
    Check if a dataset_name exists in the datasets table

    Parameters
    ----------
    dataset_name: str
        The dataset_name to search the dataset table
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    bool
        Returns True if already exists, and False if it does not exist
    """    
    result = pd.read_sql(f'SELECT count(*) ' 
                         f'FROM cvdnet_consolidated.datasets '
                         f'WHERE dataset_name = \'{dataset_name}\';',
                         con=engine).loc[0, 'count']
    
    if result == 0:
        return False
    elif result == 1:
        return True
    elif result > 1:
        raise DatasetDatabaseException("More than one of this dataset_name exists in the database")

def get_dataset_id(dataset_name, engine):
    """
    Returns a id from the dataset table for a given dataset_name

    Parameters
    ----------
    dataset_name: str
        The dataset_name to search the dataset table
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    int
        The corresponding ID for the dataset_name in the datasets table
    """
    result = pd.read_sql(f'SELECT id ' 
                         f'FROM cvdnet_consolidated.datasets '
                         f'WHERE dataset_name = \'{dataset_name}\';',
                         con=engine)
    
    if len(result.id) == 0:
        raise DatasetDatabaseException("No rows matching name found in the dataset table")
    elif len(result.id) > 1:
        raise DatasetDatabaseException("Too many rows matching name found in the dataset table")
    else:    
        return int(result.loc[0, 'id'])

def get_dataset_name(dataset_id, engine):
    """
    Returns a dataset_name from the dataset table for a given id

    Parameters
    ----------
    dataset_id: int
        The id to search the dataset table
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    str
        The corresponding dataset_name for the id in the datasets table
    """
    result = pd.read_sql(f'SELECT dataset_name ' 
                         f'FROM cvdnet_consolidated.datasets '
                         f'WHERE id = {dataset_id};',
                         con=engine)
    
    if len(result.dataset_name) == 0:
        raise DatasetDatabaseException("No rows matching ID found in the dataset table")
    elif len(result.dataset_name) > 1:
        raise DatasetDatabaseException("Too many rows matching ID found in the dataset table")
    else:    
        return result.loc[0, 'dataset_name']
    
def insert_dataset(dataset_name, engine):
    """
    Inserts a single dataset into the datasets table

    Parameters
    ----------
    dataset_name: str
        The dataset_name to be inserted into the database
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database
    
    Returns
    -------
    none
    """
    if check_dataset_name_exists(dataset_name, engine) == True:
        raise DatasetDatabaseException("dataset_name already exists in the database")

    to_insert = pd.read_sql(f'SELECT * '
                            f'FROM cvdnet_consolidated.datasets '
                            f'LIMIT 0;',
                            con=engine)
    to_insert = to_insert.drop(['id', 'date_last_updated'], axis=1)
    to_insert['dataset_name'] = [dataset_name]

    to_insert.to_sql(name='datasets',
                     con=engine,
                     if_exists='append',
                     schema="cvdnet_consolidated",
                     index=False)