# Define the data dictionary for "CMR AI measurements"
import pandas as pd
cmr_ai_measurements_data = [
    ("auto_lv_edv_index", "AI-derived left ventricular end-diastolic volume indexed to body surface area", "mL/m²"),
    ("lv_edv", "Left ventricular end-diastolic volume", "mL"),
    ("auto_lv_esv_index", "AI-derived left ventricular end-systolic volume indexed to body surface area", "mL/m²"),
    ("lv_esv", "Left ventricular end-systolic volume", "mL"),
    ("auto_lv_sv_index", "AI-derived left ventricular stroke volume indexed to body surface area", "mL/m²"),
    ("lv_sv", "Left ventricular stroke volume", "mL"),
    ("auto_lv_co", "AI-derived left ventricular cardiac output", "L/min"),
    ("lv_ef", "Left ventricular ejection fraction", "%"),
    ("auto_lv_edm_index", "AI-derived left ventricular end-diastolic mass indexed to body surface area", "g/m²"),
    ("lv_edm", "Left ventricular end-diastolic mass", "g"),
    ("lv_esm", "Left ventricular end-systolic mass", "g"),
    ("auto_vmi", "AI-derived ventricular mass index (ratio of RV to LV mass)", "Ratio"),
    ("auto_rv_edv_index", "AI-derived right ventricular end-diastolic volume indexed to body surface area", "mL/m²"),
    ("rv_edv", "Right ventricular end-diastolic volume", "mL"),
    ("auto_rv_esv_index", "AI-derived right ventricular end-systolic volume indexed to body surface area", "mL/m²"),
    ("rv_esv", "Right ventricular end-systolic volume", "mL"),
    ("auto_rv_sv_index", "AI-derived right ventricular stroke volume indexed to body surface area", "mL/m²"),
    ("rv_sv", "Right ventricular stroke volume", "mL"),
    ("auto_rv_co", "AI-derived right ventricular cardiac output", "L/min"),
    ("rv_ef", "Right ventricular ejection fraction", "%"),
    ("auto_rv_edm_index", "AI-derived right ventricular end-diastolic mass indexed to body surface area", "g/m²"),
    ("rv_edm", "Right ventricular end-diastolic mass", "g"),
    ("rv_esm", "Right ventricular end-systolic mass", "g"),
    ("auto_percent_pred_rvesvi", "AI-derived % predicted RV end-systolic volume index", "%"),
    ("auto_percent_pred_rvedvi", "AI-derived % predicted RV end-diastolic volume index", "%"),
    ("auto_percent_pred_rvef", "AI-derived % predicted RV ejection fraction", "%"),
    ("auto_percent_pred_rvsvi", "AI-derived % predicted RV stroke volume index", "%"),
    ("auto_percent_pred_rvedmi", "AI-derived % predicted RV end-diastolic mass index", "%"),
    ("auto_percent_pred_lvesvi", "AI-derived % predicted LV end-systolic volume index", "%"),
    ("auto_percent_pred_lvedvi", "AI-derived % predicted LV end-diastolic volume index", "%"),
    ("auto_percent_pred_lvef", "AI-derived % predicted LV ejection fraction", "%"),
    ("auto_percent_pred_lvsvi", "AI-derived % predicted LV stroke volume index", "%"),
    ("auto_percent_pred_lvedmi", "AI-derived % predicted LV end-diastolic mass index", "%"),
    ("auto_percent_pred_vmi", "AI-derived % predicted ventricular mass index", "%"),
    ("std_auto_percent_pred_rvef", "Standard deviation of % predicted RV ejection fraction", "%"),
    ("std_auto_percent_pred_rvesvi", "Standard deviation of % predicted RV end-systolic volume index", "%"),
    ("std_auto_percent_pred_rvedvi", "Standard deviation of % predicted RV end-diastolic volume index", "%"),
    ("std_auto_percent_pred_rvsvi", "Standard deviation of % predicted RV stroke volume index", "%"),
    ("std_auto_percent_pred_rvedmi", "Standard deviation of % predicted RV end-diastolic mass index", "%"),
    ("std_auto_percent_pred_lvef", "Standard deviation of % predicted LV ejection fraction", "%"),
    ("std_auto_percent_pred_lvedvi", "Standard deviation of % predicted LV end-diastolic volume index", "%"),
    ("std_auto_percent_pred_lvesvi", "Standard deviation of % predicted LV end-systolic volume index", "%"),
    ("std_auto_percent_pred_lvsvi", "Standard deviation of % predicted LV stroke volume index", "%"),
    ("std_auto_percent_pred_lvedmi", "Standard deviation of % predicted LV end-diastolic mass index", "%"),
    ("std_auto_percent_pred_vmi", "Standard deviation of % predicted ventricular mass index", "%")
]

# Create the DataFrame
cmr_ai_measurements_df = pd.DataFrame(cmr_ai_measurements_data, columns=["Header", "Description", "Units"])

# Generate CSV content
cmr_ai_measurements_df.to_csv('./cmr_ai_measurements.csv', index=False)
