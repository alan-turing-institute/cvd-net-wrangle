import pandas as pd
import sqlalchemy

class AnnotationsDatabaseException(Exception):
    "Database inconsistency: annotations table"

def check_category_exists(category_level_1, category_level_2, engine):
    """
    Check if a category combination (l1 and l2) exists in the annotations table

    Parameters
    ----------
    category_level_1: str
        The category_level_1 to search the annotations table
    category_level_2: str or None
        The category_level_2 to search the annotations table. This should be 'None' when not present
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    bool
        Returns True if already exists, and False if it does not exist
    """
    if pd.isna(category_level_2) == True:
        result = pd.read_sql(f'SELECT count(*) ' 
                             f'FROM cvdnet_consolidated.annotations '
                             f'WHERE category_level_1 = \'{category_level_1}\' and category_level_2 is null;',
                             con=engine).loc[0, 'count']
    else:
        result = pd.read_sql(f'SELECT count(*) ' 
                             f'FROM cvdnet_consolidated.annotations '
                             f'WHERE category_level_1 = \'{category_level_1}\' and category_level_2 = \'{category_level_2}\';',
                             con=engine).loc[0, 'count']
    
    if result == 0:
        return False
    elif result == 1:
        return True
    elif result > 1:
        raise AnnotationsDatabaseException("More than one of this category combination exists in the database")

def get_annotation_id(category_level_1, category_level_2, engine):
    """
    Returns a id from the annotations table for a given category combination (l1 and l2)

    Parameters
    ----------
    category_level_1: str
        The category_level_1 to search the annotations table
    category_level_2: str or None
        The category_level_2 to search the annotations table. This should be 'None' when not present
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    int
        The corresponding ID for the category combination in the annotations table
    """
    if pd.isna(category_level_2) == True:
        result = pd.read_sql(f'SELECT id ' 
                             f'FROM cvdnet_consolidated.annotations '
                             f'WHERE category_level_1 = \'{category_level_1}\' and category_level_2 is null;',
                             con=engine)
    else:
        result = pd.read_sql(f'SELECT id ' 
                             f'FROM cvdnet_consolidated.annotations '
                             f'WHERE category_level_1 = \'{category_level_1}\' and category_level_2 = \'{category_level_2}\';',
                             con=engine)
    
    if len(result.id) == 0:
        raise AnnotationsDatabaseException("No rows matching name found in the annotations table")
    elif len(result.id) > 1:
        raise AnnotationsDatabaseException("Too many rows matching category combination found in the annotations table")
    else:    
        return int(result.loc[0, 'id'])

def get_category_levels(annotation_id, engine):
    """
    Returns a list of category levels 1 and 2 from the annotations table for a given id

    Parameters
    ----------
    annotation_id: int
        The id to search the annotations table
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    list
        List of two strings - category levels 1 and 2
    """
    result = pd.read_sql(f'SELECT category_level_1, category_level_2 ' 
                         f'FROM cvdnet_consolidated.annotations '
                         f'WHERE id = {annotation_id};',
                         con=engine)
    
    if len(result) == 0:
        raise AnnotationsDatabaseException("No rows matching ID found in the annotations table")
    elif len(result) > 1:
        raise AnnotationsDatabaseException("Too many rows matching ID found in the annotations table")
    else:    
        return list(result)
    
def insert_annotations(annotation_df, engine):
    """
    Using a dataframe of category level 1 and level 2 combinations, new annotations are inserted into the annotations table

    Parameters
    ----------
    annotation_df: dataframe
        A dataframe with 2 columns ('category_level_1' and 'category_level_2') containing new category combinations only
    engine: a sqlalchemy engine
        A sqlalchemy engine for connecting to the CVD-Net database

    Returns
    -------
    None
    """
    # Validate the data in the annotation_df dataframe
    annotations_template = pd.read_csv('templates/annotations_template.csv')
    
    if any(annotations_template.columns != annotation_df.columns):
        raise AnnotationsDatabaseException("Format of annotations dataframe does not match template")

    # All categories converted to upper case to avoid case mismatches creating duplicates
    annotation_df['category_level_1'] = annotation_df['category_level_1'].str.strip().str.upper()
    annotation_df['category_level_2'] = annotation_df['category_level_2'].str.strip().str.upper()

    # Ensure no blank strings in dataframe
    if any(annotation_df.eq('').any(axis=1)) == True:
        raise AnnotationsDatabaseException("Blank strings in dataframe")
    
    # Ensure level 1 is not null
    if any(annotation_df['category_level_1'].isna()):
        raise AnnotationsDatabaseException("Category level 1 cannot contain NAs")

    # Level 1 must be populated if level 2 is
    if any(annotation_df['category_level_2'].notna() & annotation_df['category_level_1'].isna()):
        raise AnnotationsDatabaseException("category_level_2 cannot be populated if category_level_1 is not")

    # Check for duplicated rows
    if (any(annotation_df.duplicated()) == True):
        raise AnnotationsDatabaseException("Duplicated rows present in the dataframe")
    
    # Check is any category combinations already exist
    for index, row in annotation_df.iterrows():
        if check_category_exists(row['category_level_1'],
                                 row['category_level_2'],
                                 engine) == True:
            raise AnnotationsDatabaseException("A row already exists in the database")

    # Now insert into database
    annotation_df.to_sql(name='annotations',
                       con=engine,
                       if_exists='append',
                       schema='cvdnet_consolidated',
                       index=False)
