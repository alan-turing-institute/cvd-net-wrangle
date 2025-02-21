import database_interaction
import datasets
import annotations
import metadata
import transform_raw_data
import subjects
import measurements

# Loading this script will import all other parts of the pipeline
# The database engine is generated upon loading
# All other functions can be called after running this script

engine = database_interaction.connect_database()

