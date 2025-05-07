# Define the data dictionary for "Functional tests and E10"
import pandas as pd

functional_tests_data = [
    ("xnat_id", "Unique identifier for patient in the imaging system (XNAT)", "N/A"),
    ("mri_date", "Date of the MRI scan associated with functional data", "Date (YYYY-MM-DD)"),
    ("heart_rate", "Heart rate at time of test or exam", "beats per minute (bpm)"),
    ("who_functional_class", "WHO functional class of pulmonary hypertension (I-IV)", "Ordinal (I–IV)"),
    ("esc_ers_score", "Risk score based on ESC/ERS guidelines", "Score"),
    ("reveal_score", "REVEAL 2.0 risk score for pulmonary hypertension", "Score"),
    ("reveal_score_lite", "REVEAL Lite 2 risk score", "Score"),
    ("compera_score", "COMPERA risk score", "Score"),
    ("date_of_pft", "Date of pulmonary function test", "Date (YYYY-MM-DD)"),
    ("fev1", "Forced expiratory volume in 1 second", "L"),
    ("fvc", "Forced vital capacity", "L"),
    ("tlco", "Transfer factor for carbon monoxide", "mmol/min/kPa"),
    ("percent_predict_fev1", "Percent of predicted FEV1 value", "%"),
    ("percent_predict_fvc", "Percent of predicted FVC value", "%"),
    ("percent_predict_tlco", "Percent of predicted TLCO value", "%"),
    ("date_iswt", "Date of Incremental Shuttle Walk Test (ISWT)", "Date (YYYY-MM-DD)"),
    ("walking_distance", "Distance walked during ISWT", "meters"),
    ("pericardial_effusion", "Presence of pericardial effusion on imaging", "Boolean (Yes/No)"),
    ("date_of_qo_lq", "Date of EmPHasis-10 questionnaire assessment", "Date (YYYY-MM-DD)"),
    ("qo_l_score", "EmPHasis-10 quality of life score (0-50)", "Score (0–50)")
]

# Create the DataFrame
functional_df = pd.DataFrame(functional_tests_data, columns=["Header", "Description", "Units"])

# Generate CSV content
functional_df.to_csv('./aspire_functional_tests_e10.csv', index=False)
