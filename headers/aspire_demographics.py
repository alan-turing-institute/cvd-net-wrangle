import pandas as pd

# Define the data dictionary based on provided headers and context
data = [
    ("xnat_id", "Unique identifier for patient in the imaging system (XNAT)", "N/A"),
    ("mri_date", "Date of the MRI scan", "Date (YYYY-MM-DD)"),
    ("mri_order", "Ordering of MRI relative to timeline (e.g., baseline, follow-up)", "Ordinal / Text"),
    ("baseline_prevalent", "Indicates if this is the baseline (incident) or prevalent case", "Boolean (Yes/No)"),
    ("sex", "Biological sex of the patient", "Categorical (Male/Female)"),
    ("age", "Age of the patient at MRI or diagnosis", "Years"),
    ("ethnicity", "Ethnic background of the patient", "Text / Categorical"),
    ("bsa", "Body surface area", "m²"),
    ("weight", "Body weight of the patient", "kg"),
    ("date_referral", "Date patient was referred to the PH center", "Date (YYYY-MM-DD)"),
    ("date_diagnosis", "Date of final PH diagnosis", "Date (YYYY-MM-DD)"),
    ("pah_subcategory", "Specific PAH subtype if applicable", "Text / Categorical"),
    ("final_primary_ph_diagnosis", "Final confirmed diagnosis of pulmonary hypertension", "Text / Categorical"),
    ("final_primary_diagnosis_subcategory", "Subcategory of the primary PH diagnosis", "Text / Categorical"),
    ("additional_cause_of_ph", "Additional contributing cause of pulmonary hypertension", "Text / Categorical"),
    ("additional_cause_of_ph_subcategory", "Subcategory of additional cause", "Text / Categorical"),
    ("other_additional_cause_of_ph", "Other non-standard contributing PH cause", "Text / Categorical"),
    ("other_additional_cause_of_ph_subcategory", "Subcategory of other additional cause", "Text / Categorical"),
    ("date_census_death", "Date of mortality census or death", "Date (YYYY-MM-DD)"),
    ("survival_time", "Time from diagnosis or baseline to death or last follow-up", "Days or Years"),
    ("death_1y", "Patient death within 1 year of diagnosis or MRI", "Boolean (Yes/No)"),
    ("overall_death", "Whether the patient is deceased at latest follow-up", "Boolean (Yes/No)")
]

# Create the DataFrame
df = pd.DataFrame(data, columns=["Header", "Description", "Units"])

# Show the CSV content
df.to_csv('./aspire_demographics.csv', index=False)
