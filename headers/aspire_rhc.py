import pandas as pd

rhc_data = [
    ("xnat_id", "Unique identifier for patient in the imaging system (XNAT)", "N/A"),
    ("mri_date", "Date of the MRI scan associated with RHC data", "Date (YYYY-MM-DD)"),
    ("pcw_mean", "Mean pulmonary capillary wedge pressure (PCWP)", "mmHg"),
    ("pvr", "Pulmonary vascular resistance", "Wood units"),
    ("pa_mean", "Mean pulmonary artery pressure", "mmHg"),
    ("pa_systolic", "Systolic pulmonary artery pressure", "mmHg"),
    ("pa_diastolic", "Diastolic pulmonary artery pressure", "mmHg"),
    ("ra_mean", "Mean right atrial pressure", "mmHg"),
    ("arterial_systolic", "Systolic systemic arterial pressure", "mmHg"),
    ("arterial_diastolic", "Diastolic systemic arterial pressure", "mmHg"),
    ("arterial_mean", "Mean systemic arterial pressure", "mmHg"),
    ("sa_o2", "Systemic arterial oxygen saturation", "%"),
    ("svo2", "Mixed venous oxygen saturation", "%"),
    ("cardiac_output", "Cardiac output measured during RHC", "L/min"),
    ("cardiac_index", "Cardiac output normalized to body surface area", "L/min/m²"),
    ("heart_rate_rhc", "Heart rate at the time of RHC", "beats per minute (bpm)"),
    ("rhc_stroke_volume", "Stroke volume measured during RHC", "mL/beat")
]

# Create the DataFrame
rhc_df = pd.DataFrame(rhc_data, columns=["Header", "Description", "Units"])

# Show the CSV content

rhc_df.to_csv('./aspire_rhc.csv', index=False)
