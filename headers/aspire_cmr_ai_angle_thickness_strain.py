# Define the data dictionary for "CMR AI Angle Thickness Strain"
import pandas as pd
cmr_ai_data = [
    ("xnat_id", "Unique identifier for patient in the imaging system (XNAT)", "N/A"),
    ("mri_date", "Date of the MRI scan", "Date (YYYY-MM-DD)"),
    ("min_septal_angle", "Minimum interventricular septal angle during cardiac cycle", "Degrees (°)"),
    ("max_septal_angle", "Maximum interventricular septal angle during cardiac cycle", "Degrees (°)"),
    ("wted_septum", "Wall thickness at end-diastole of the septum", "mm"),
    ("wtes_septum", "Wall thickness at end-systole of the septum", "mm"),
    ("wted_lv", "Wall thickness at end-diastole of the left ventricle", "mm"),
    ("wtes_lv", "Wall thickness at end-systole of the left ventricle", "mm"),
    ("wted_rv", "Wall thickness at end-diastole of the right ventricle", "mm"),
    ("wtes_rv", "Wall thickness at end-systole of the right ventricle", "mm"),
    ("lv_min_4ch_strain", "Minimum longitudinal strain of the left ventricle in 4-chamber view", "%"),
    ("lv_max_4ch_strain", "Maximum longitudinal strain of the left ventricle in 4-chamber view", "%"),
    ("rv_min_4ch_strain", "Minimum longitudinal strain of the right ventricle in 4-chamber view", "%"),
    ("rv_max_4ch_strain", "Maximum longitudinal strain of the right ventricle in 4-chamber view", "%"),
    ("la_min_4ch_strain", "Minimum longitudinal strain of the left atrium in 4-chamber view", "%"),
    ("la_max_4ch_strain", "Maximum longitudinal strain of the left atrium in 4-chamber view", "%"),
    ("ra_min_4ch_strain", "Minimum longitudinal strain of the right atrium in 4-chamber view", "%"),
    ("ra_max_4ch_strain", "Maximum longitudinal strain of the right atrium in 4-chamber view", "%")
]

# Create the DataFrame
cmr_ai_df = pd.DataFrame(cmr_ai_data, columns=["Header", "Description", "Units"])

# Generate CSV content
cmr_ai_df.to_csv('./aspire_cmr_ai_angle_thickness_strain.csv', index=False)
