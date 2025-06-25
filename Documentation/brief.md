## Clinical and Physiological Data from PH Monitoring Study

### Background

**Fit-PH** is a dataset derived from _PHoenix_, a Phase IV randomized clinical trial involving patients with pulmonary hypertension (PH). The trial's primary aim was to assess dose-response effects and clinical efficacy of two medications: **riociguat** and **selexipag**, in addition to standard care. As part of the study, participants were also implanted with cardiovascular monitoring devices to enabke continuous remote capture of physiological signals. 

The dataset was collected with the aim of developing personalized treatment-response profiles, by correlating each individual's device-measured physiology with medication dosing and outcomes. Remote monitoring detects early signs of therapeutic response -or adverse changes- to enable timely adjustment strategies. 

The patient cohort comprises of 17 individuals diagnosed with PH who enrolled in the clinical trial and were randomised to receive varying doses of riociguat or selexipag. Trial design shows that patients were monitored over a 27 week period during which they were required to attend 4 hospital visits. Monitoring of physiological, functional and psychological wellbeing were achieved by patients undergoing clinical assessments at clinic, or through self-reporting questionaires. Additional data were also recorded remotely from home through implantable devices that tracked continuous physiological signals e.g. blood pressure. 

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
    - **Functional test results** (e.g. WHO functional class, 6 minute walk test)
    - **Patient-reported outcomes from questionaires** (e.g. EQ-5D-5L, EmPHasis-10) 
    - **Physiological vital signs** (e.g. resting blood pressure, NT-proBNP)
- **Implant temporal data:** (e.g pulmonary artery presssure) -- physiological time-series from implantable devices

Figure 2 shows the timeline of data collection for each participant in the PHonix trial, highlighting the timing and key data collected. Each row represents a patient’s 29-week participation in the study, with colored segments and markers indicating different data modalities. Continuous bands represent daily remote monitoring from implantable devices (e.g. pulmonary artery pressure, cardiac output, and physical activity), while discrete markers denote in-clinic visits, clinical assessments, and therapeutic interventions. [For example, red circles indicate hospital visits, blue squares indicate questionnaire completions, and yellow triangles represent dose adjustments.] This visualisation illustrates the structured and multimodal nature of the dataset across the study period.

| ![](data_timeline.jpg) |
|:--:|
| *Figure 2: Data collection timeline of key data for PHoenix participants* |

#### Demographics 

Participant demographics data were recorded at enrollment. The PHoenix trial enrolled 17 patients with pulmonary arterial hypertension (PAH). The median age was x years, and x% were female. Most participants were classified as WHO functional class III, reflecting moderate to severe limitations in physical activity. The cohort included patients from a range of socioeconomic backgrounds, and Index of Multiple Deprivation (IMD) scores were recorded to assess deprivation levels. Small sample size limits conclusions about potential bias in key demographic variables.

| ![](demographics.jpg) |
|:--:|
| *Figure 3: Patient demography key variables, recorded at enrollment* |

#### Medical History

Patient medical history data in the PHonix trial included information on the time since PAH diagnosis, aetiology of disease, and pre-existing comorbidities. Details of prior PAH treatments were also recorded, including background therapies taken before trial enrolment. These variables provide clinical context for interpreting treatment response and disease progression across the trial period.

| ![](history.jpg) |
|:--:|
| *Figure 3: Patient medical history key variables, recorded at enrollment* |

#### Clinical assessments

##### Functional tests
##### Patient-reported outcomes
##### Physiological measurements



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
