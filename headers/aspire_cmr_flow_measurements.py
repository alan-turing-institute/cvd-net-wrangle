# Define the data dictionary for "CMR flow measurements"
import pandas as pd
cmr_flow_measurements_data = [
    ("pa_relative_area_change", "Relative area change of pulmonary artery during cardiac cycle", "%"),
    ("diastolic_pa_area", "Pulmonary artery area during diastole", "mm²"),
    ("systolic_pa_area", "Pulmonary artery area during systole", "mm²"),
    ("pa_forward_flow_volume", "Volume of forward flow through the pulmonary artery", "mL"),
    ("pa_forward_flow_per_min", "Rate of forward flow through the pulmonary artery per minute", "mL/min"),
    ("pa_backward_flow_volume", "Volume of backward flow through the pulmonary artery", "mL"),
    ("pa_backward_flow_per_min", "Rate of backward flow through the pulmonary artery per minute", "mL/min"),
    ("pa_peak_flow_velocity", "Peak flow velocity in the pulmonary artery", "cm/s"),
    ("pa_peak_pressure_gradient", "Peak pressure gradient across the pulmonary artery", "mmHg"),
    ("pa_mean_pressure_gradient", "Mean pressure gradient across the pulmonary artery", "mmHg"),
    ("pa_heart_beat_interval", "Interval between heartbeats during PA flow measurement", "ms"),
    ("pa_flow_per_heart_beat", "Volume of pulmonary artery flow per heartbeat", "mL"),
    ("pa_flow_per_min", "Pulmonary artery flow rate per minute", "mL/min"),
    ("pa_regurgitant_fraction", "Fraction of blood that flows backward in pulmonary artery", "%"),
    ("ao_qflow_average", "Average flow through the aorta", "mL/s"),
    ("aa_peak_flow_velocity", "Peak flow velocity in the ascending aorta", "cm/s"),
    ("aa_peak_pressure_gradient", "Peak pressure gradient in the ascending aorta", "mmHg"),
    ("aa_mean_pressure_gradient", "Mean pressure gradient in the ascending aorta", "mmHg"),
    ("aa_heart_beat_interval", "Interval between heartbeats during aortic flow measurement", "ms"),
    ("aa_flow_per_heart_beat", "Volume of aortic flow per heartbeat", "mL"),
    ("aa_flow_per_min", "Aortic flow rate per minute", "mL/min"),
    ("aa_min_vessel_area", "Minimum area of the ascending aorta", "mm²"),
    ("aa_max_vessel_area", "Maximum area of the ascending aorta", "mm²"),
    ("aa_regurgitant_fraction", "Fraction of blood that flows backward in ascending aorta", "%"),
    ("aa_forward_flow_volume", "Volume of forward flow through the ascending aorta", "mL"),
    ("aa_forward_flow_per_min", "Rate of forward flow through the ascending aorta per minute", "mL/min"),
    ("aa_backward_flow_volume", "Volume of backward flow through the ascending aorta", "mL"),
    ("aa_backward_flow_per_min", "Rate of backward flow through the ascending aorta per minute", "mL/min"),
    ("septal_angle_syst", "Septal angle during systole", "Degrees (°)"),
    ("septal_angle_diast", "Septal angle during diastole", "Degrees (°)")
]

# Create the DataFrame
cmr_flow_df = pd.DataFrame(cmr_flow_measurements_data, columns=["Header", "Description", "Units"])

# Generate CSV content

cmr_flow_df.to_csv('./aspire_cmr_flow_measurements.csv', index=False)
