# Re-importing after code environment reset
import pandas as pd
import io

# Define the data dictionary for "CMR AI PA Flow"
cmr_ai_pa_flow_data = [
    ("pa_ai_relative_area_change", "Relative area change of the pulmonary artery across the cardiac cycle", "%"),
    ("net_forward_flow_over_full_rr_interval", "Net forward blood flow through the pulmonary artery over one full RR interval", "mL"),
    ("net_backward_flow_over_full_rr_interval", "Net backward flow through the pulmonary artery over one full RR interval", "mL"),
    ("average_flow_velocity", "Average velocity of blood flow in the pulmonary artery", "cm/s"),
    ("maximum_flow_velocity", "Maximum velocity of blood flow in the pulmonary artery", "cm/s"),
    ("trigger_time_of_moment_of_peak_flow", "Time point of peak systolic flow as a trigger delay from R-wave", "ms"),
    ("trigger_time_of_end_of_systolic_phase", "Time point marking end of systolic phase", "ms"),
    ("minimum_pa_contour_area", "Minimum cross-sectional area of the pulmonary artery", "mm²"),
    ("maximum_pa_contour_area", "Maximum cross-sectional area of the pulmonary artery", "mm²"),
    ("flow_displacement_at_moment_of_peak_systolic_flow", "Flow displacement from center at peak systolic flow", "mm"),
    ("maximum_flow_displacement_within_systolic_phase", "Maximum displacement of flow center during systole", "mm"),
    ("flow_reversal_ratio", "Ratio of backward to forward flow volumes", "Unitless (Ratio)"),
    ("net_forward_flow_during_systolic_phase", "Net forward flow through PA during systolic phase", "mL"),
    ("forward_flow_during_systolic_phase", "Total forward flow through PA during systole", "mL"),
    ("backward_flow_during_systolic_phase", "Total backward flow through PA during systole", "mL")
]

# Create the DataFrame
cmr_ai_pa_flow_df = pd.DataFrame(cmr_ai_pa_flow_data, columns=["Header", "Description", "Units"])

# Generate CSV content
cmr_ai_pa_flow_df.to_csv('./aspire_cmr_ai_pa_flow.csv', index=False)
