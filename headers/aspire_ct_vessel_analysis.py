import pandas as pd
ct_vessel_data = [
    ("peel_vessel_vol_left_15", "Volume of peripheral vessels in the left lung within the outermost 15% of lung radius", "mL"),
    ("peel_vessel_vol_right_15", "Volume of peripheral vessels in the right lung within the outermost 15% of lung radius", "mL"),
    ("peel_vessel_vol_total_15", "Total peripheral vessel volume (left + right) within 15% lung radius", "mL"),
    ("peel_vessel_vol_left_30", "Volume of peripheral vessels in the left lung within the outermost 30% of lung radius", "mL"),
    ("peel_vessel_vol_right_30", "Volume of peripheral vessels in the right lung within the outermost 30% of lung radius", "mL"),
    ("peel_vessel_vol_total_30", "Total peripheral vessel volume (left + right) within 30% lung radius", "mL"),
    ("peel_vessel_vol_left_45", "Volume of peripheral vessels in the left lung within the outermost 45% of lung radius", "mL"),
    ("peel_vessel_vol_right_45", "Volume of peripheral vessels in the right lung within the outermost 45% of lung radius", "mL"),
    ("peel_vessel_vol_total_45", "Total peripheral vessel volume (left + right) within 45% lung radius", "mL"),
    ("lung_vol_total", "Total lung volume derived from CT scan", "mL"),
    ("vessel_vol_total", "Total intrapulmonary vessel volume", "mL"),
    ("peripheral_vessel_vol_perc_1", "Peripheral vessel volume as a percentage of total vessel volume (zone 1)", "%"),
    ("peripheral_vessel_vol_perc_2", "Peripheral vessel volume as a percentage of total vessel volume (zone 2)", "%"),
    ("peripheral_vessel_vol_perc_3", "Peripheral vessel volume as a percentage of total vessel volume (zone 3)", "%"),
    ("peripheral_vessel_vol_perc_4", "Peripheral vessel volume as a percentage of total vessel volume (zone 4)", "%"),
    ("peripheral_vessel_vol_perc_5", "Peripheral vessel volume as a percentage of total vessel volume (zone 5)", "%")
]

# Create the DataFrame
ct_vessel_df = pd.DataFrame(ct_vessel_data, columns=["Header", "Description", "Units"])

# Generate CSV content
ct_vessel_df.to_csv('./aspire_ct_vessel_analysis.csv', index=False)
