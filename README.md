# CVD-Net Data Wrangling

CVD-Net (Network of Cardiovascular Digital Twins) is a multi-year collaboration between The Alan Turing Institute, Imperial College London, University of Nottingham, and University of Sheffield. Its aim is to create digital twins of the hearts of a group of patients suffering from pulmonary arterial hypertension and to demonstrate their use in a clinical care pathway. 

This work is supported by the UK Engineering and Physical Sciences Research Council (EPSRC) Grant EP/Z531297/1. 

This repository contains outputs from Work Package 2 of 6 ("Digital Tapestry and Infrastructure.")

This repository is a *work in progress*, currently containing a draft data wrangling pipeline. We are sharing this code base early to promote collaboration and enhance transparency. 

## Navigate the directories

- [dummy data](dummy_data/): Data dictionaries for ASPIRE and FIT-PH (created with our best guess of what the variables are - awaiting confirmation from dataset provider), and scripts to generate dummy datasets for both.
- [headers](headers/): Blank header files for ASPIRE and FIT-PH showing the structure of the raw data.
- [pipeline](pipeline/): The codebase for the data wrangling pipeline. This contains a PostgreSQL database schema and a Python codebase for validating data, loading data and inserting it into the database.
  - Extensive [documentation](pipeline/documentation/pipeline_documentation.md) gives a detailed overview of how the pipeline works, how it is run, and why certain decisions were made.
  - An interactive [tutorial notebook](pipeline/tutorial.ipynb) shows how the pipeline is run from start to finish. 

These files are currently blank (but are needed for the pipeline to run): 
- [aspire_dt_data_frame_headers.csv](headers/aspire_dt_data_frame_headers.csv)
- [fit_ph_linq_cm_data_frame_headers.csv](headers/fit_ph_linq_cm_data_frame_headers.csv)
- [ASPIRE_dictionary_to_template.csv](dummy_data/ASPIRE_dictionary_to_template.csv)
- [FIT-PH_dictionary_to_template.csv](dummy_data/FIT-PH_dictionary_to_template.csv)
- [generate_ASPIRE_dummy_data.py](dummy_data/generate_ASPIRE_dummy_data.py)
- [generate_FIT-PH_dummy_data.py](dummy_data/generate_FIT-PH_dummy_data.py)
- [transform_raw_data.py](pipeline/transform_raw_data.py)

## Contributors 

### Past Contributors
- Daniel Delbarre
  - Led the planning and implementation of the first working pipeline based on dummy data (ASPIRE and FIT-PH), thoroughly documenting the decisions taken to make this code base and how to use it. Planned the pipeline so that further datasets could be included at later stages (once transformed to the harmonised format). Considered both database developers and users (researchers using this data for their model development).      

### Current Contributors
- Rachael Stickland (until 6th March 2025)
- May Young
- Mahwish Mohammad
- Luis Santos

## Contact 

For any questions contact Camila Rangel Smith (crangelsmith@turing.ac.uk) and/or Mahwish Mohammad (mmohammad@turing.ac.uk), or feel free to submit a GitHub Issue. 

## License 

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.  
For more information, refer to [GNU General Public License](https://www.gnu.org/licenses/gpl-3.0.en.html).
