# Re-import necessary libraries after environment reset
import pandas as pd
import io

# Define the data dictionary for "CMR AI Atrial Data"
cmr_ai_atrial_data = [
    ("la_max_2ch_area", "Maximum left atrial area in 2-chamber view", "cm²"),
    ("la_min_2ch_area", "Minimum left atrial area in 2-chamber view", "cm²"),
    ("la_max_4ch_area", "Maximum left atrial area in 4-chamber view", "cm²"),
    ("la_min_4ch_area", "Minimum left atrial area in 4-chamber view", "cm²"),
    ("ra_max_2ch_area", "Maximum right atrial area in 2-chamber view", "cm²"),
    ("ra_min_2ch_area", "Minimum right atrial area in 2-chamber view", "cm²"),
    ("ra_max_4ch_area", "Maximum right atrial area in 4-chamber view", "cm²"),
    ("ra_min_4ch_area", "Minimum right atrial area in 4-chamber view", "cm²"),
    ("la_max_2ch_length", "Maximum left atrial length in 2-chamber view", "mm"),
    ("la_min_2ch_length", "Minimum left atrial length in 2-chamber view", "mm"),
    ("la_max_4ch_length", "Maximum left atrial length in 4-chamber view", "mm"),
    ("la_min_4ch_length", "Minimum left atrial length in 4-chamber view", "mm"),
    ("ra_max_2ch_length", "Maximum right atrial length in 2-chamber view", "mm"),
    ("ra_min_2ch_length", "Minimum right atrial length in 2-chamber view", "mm"),
    ("ra_max_4ch_length", "Maximum right atrial length in 4-chamber view", "mm"),
    ("ra_min_4ch_length", "Minimum right atrial length in 4-chamber view", "mm"),
    ("la_max_volume", "Maximum left atrial volume", "mL"),
    ("la_min_volume", "Minimum left atrial volume", "mL"),
    ("la_ejection_fraction", "Left atrial ejection fraction", "%"),
    ("la_long_axis_strain", "Left atrial longitudinal strain", "%"),
    ("ra_max_volume", "Maximum right atrial volume", "mL"),
    ("ra_min_volume", "Minimum right atrial volume", "mL"),
    ("ra_ejection_fraction", "Right atrial ejection fraction", "%"),
    ("ra_long_axis_strain", "Right atrial longitudinal strain", "%"),
    ("la_max_volume_index", "Maximum left atrial volume indexed to body surface area", "mL/m²"),
    ("la_min_volume_index", "Minimum left atrial volume indexed to body surface area", "mL/m²"),
    ("la_long_axis_strain_index", "Left atrial longitudinal strain indexed to body surface area", "%/m²"),
    ("ra_max_volume_index", "Maximum right atrial volume indexed to body surface area", "mL/m²"),
    ("ra_min_volume_index", "Minimum right atrial volume indexed to body surface area", "mL/m²"),
    ("ra_long_axis_strain_index", "Right atrial longitudinal strain indexed to body surface area", "%/m²")
]

# Create the DataFrame
cmr_ai_atrial_df = pd.DataFrame(cmr_ai_atrial_data, columns=["Header", "Description", "Units"])

# Generate CSV content
cmr_ai_atrial_df.to_csv('./cmr_ai_atrial.csv', index=False)