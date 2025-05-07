import pandas as pd
import io

# Define the data dictionary for "Blood tests"
blood_tests_data = [
    ("xnat_id", "Unique identifier for patient in the imaging system (XNAT)", "N/A"),
    ("mri_date", "Date of the MRI scan associated with blood test data", "Date (YYYY-MM-DD)"),
    ("bnp_date", "Date of B-type natriuretic peptide (BNP) test", "Date (YYYY-MM-DD)"),
    ("bnp", "B-type natriuretic peptide level", "pg/mL"),
    ("egfr_date", "Date of estimated glomerular filtration rate (eGFR) test", "Date (YYYY-MM-DD)"),
    ("egfr", "Estimated glomerular filtration rate", "mL/min/1.73m²"),
    ("blood_test_date", "Date general blood tests were taken", "Date (YYYY-MM-DD)"),
    ("urea", "Urea concentration in blood", "mmol/L"),
    ("sodium", "Sodium level in blood", "mmol/L"),
    ("total_bilirubin", "Total bilirubin concentration", "µmol/L"),
    ("alkaline_phosphatase", "Alkaline phosphatase level", "IU/L"),
    ("ggt", "Gamma-glutamyl transferase level", "IU/L"),
    ("albumin", "Albumin concentration in blood", "g/L"),
    ("creatinine", "Creatinine level in blood", "µmol/L"),
    ("alt", "Alanine aminotransferase level", "IU/L"),
    ("ast", "Aspartate aminotransferase level", "IU/L")
]

# Create the DataFrame
blood_df = pd.DataFrame(blood_tests_data, columns=["Header", "Description", "Units"])

# Generate CSV content

blood_df.to_csv('./aspire_blood_tests.csv', index=False)
