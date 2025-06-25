## Clinical and Physiological Data from PH Monitoring Study

### Background

**Fit-PH** is a dataset derived from _PHoenix_, a Phase IV randomized clinical trial involving patients with pulmonary hypertension (PH). The trial's primary aim was to assess dose-response effects and clinical efficacy of two medications: **riociguat** and **selexipag**, in addition to standard care. As part of the study, participants were also implanted with cardiovascular monitoring devices to enabke continuous remote capture of physiological signals. 

The dataset was collected with the aim of developing personalized treatment-response profiles, by correlating each individual's device-measured physiology with medication dosing and outcomes. Remote monitoring detects early signs of therapeutic response -or adverse changes- to enable timely adjustment strategies. 

The patient cohort comprises of 17 individuals diagnosed with PH who enrolled in the clinical trial and were randomised to receive varying doses of riociguat or selexipag. Patients were monitored over a 27 week period during which they were required to attend 4 hospital visits. At each visit, they underwent clinical assessments for physiological, functional and psychological wellbeing. 

Additional data were also recorded remotely from home, through self-administered tests and implantable devices that tracked continuous physiological signals e.g. blood pressure. Clinical trial outcomes - functional status, adverse events were linked to both in-clinic and remotely acquired measurements.  

| ![](PHoenix.png) |
|:--:|
| *Figure 1: Summary of PHoenix trial assessments published by Clinical Research & Innovation Office* |

<div style="page-break-after: always;"></div>

### Data Composition

Participants in the PHoenix trial were monitored over a 27-week period, during which both several types of data were collected for each participant. The data can be grouped as follows:

- **Demographic:** (e.g. age) recorded at baseline 
- **Medical History:** (e.g. date of diagnoses) recorded at baseline 
- **Intervention regime:** (e.g. riociguat 5mg daily) Date-stamped records of medication regime, titrations and changes
- **Clinical assessments:** 
    - **Functional test results** (e.g. WHO functional class), recorded at baseline and periodically during followup visits
    - **Patient-reported outcomes from questionaires** (e.g. ED5D5L) collected at x intervals
- **Implant temporal data:** (e.g blood pressure) physiological time-series from implantable devices

Figure 2 shows the timeline of data collection for each participant in the PHonix trial, showing the timing and key types of data collected. Each row represents one patient’s 29-week participation, with colored segments and markers indicating different data modalities. (Say something about what each of the markers mean.)

| ![](data_timeline.jpg) |
|:--:|
| *Figure 2: Data collection timeline of key data for PHoenix participants* |

#### Demographics 

Participant demographics data were recorded at enrollment. The PHoenix trial enrolled 17 patients with pulmonary arterial hypertension (PAH). The median age was x years, and x% were female. Most participants were classified as WHO functional class III, reflecting moderate to severe limitations in physical activity. The cohort included patients from a range of socioeconomic backgrounds, and Index of Multiple Deprivation (IMD) scores were recorded to assess deprivation levels. Small sample size limits conclusions about potential bias in key demographic variables.


| ![](demographics.jpg) |
|:--:|
| *Figure 3: Patient demography key variables recorded at enrollment* |


<!-- 
- What entities are being measured (e.g., patients, hospital visits)?
- What are the key variables/columns?
- What is the size and structure (e.g., number of rows, time range, frequency)? (Heatmap?)
- Are there distinct cohorts, subgroups, or timepoints? (put histogram of diagnosis types?)

## Data Collection Context
- Are there known biases in how the data was collected? (histograms? can't do ethnicity, what about sex and age?)

## Data Sensitivity and Privacy
## Data Missingness 
-->
<div style="page-break-after: always;"></div>

### References

1. [Pulmonary Hypertension: Intensification and Personalization of Combination Rx (PHoenix): A phase IV randomized trial for the evaluation of dose‐response and clinical efficacy of riociguat and selexipag using implanted technologies ](https://pmc.ncbi.nlm.nih.gov/articles/PMC10945040/)
2. [Remote monitored physiological response to therapeutic escalation and clinical worsening in patients with pulmonary arterial hypertension](https://www.medrxiv.org/content/10.1101/2023.04.27.23289153v2)
3. [Data Dictionaries](https://thealanturininstitute.sharepoint.com/:x:/r/sites/cvdnetshared/_layouts/15/Doc.aspx?sourcedoc=%7B7D68FCD0-6969-4BD6-BE8D-1251DB4CB34C%7D&file=Co-WIP%20Data%20Dictionaries%20(FitPH%20%26%20ASPIRE).xlsx&action=default&mobileredirect=true)
