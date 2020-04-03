# Mapping Unstructured Medication Notes to a Standardized Generic Drug Resource for Machine Learning Research

## Introduction

In this repository, the goal is to map medication notes in Electronic Health Records to a standard resource of generic medications that is not specific to a single hospital/dataset. This allows the comparison between different datasets and supports the development of machine learning in multi-centre studies. 


**AEOLUS** (Adverse Event Open Learning through Universal Standardization) – is an open-source dataset used for the identification of adverse drug reactions (ADRs).  In AEOLUS, different strategies for data cleaning and normalization were applied: removing duplicate case records, applying standardized vocabularies with drug names mapped to RxNorm concepts and outcomes mapped to SNOMED-CT concepts. *[Banda, J., Evans, L., Vanguri, R. et al. A curated and standardized adverse drug event resource to accelerate drug safety research. Sci Data 3, 160026 (2016). https://doi.org/10.1038/sdata.2016.26]*


To map medication entries to AEOLUS, the generic drug name should be extracted from the medication entries. For demonstration purposes on a publically available dataset, we map the medications in the MIMIC III dataset *[MIMIC-III, a freely accessible critical care database. Johnson AEW, Pollard TJ, Shen L, Lehman L, Feng M, Ghassemi M, Moody B, Szolovits P, Celi LA, and Mark RG. Scientific Data (2016). DOI: 10.1038/sdata.2016.35. Available at: http://www.nature.com/articles/sdata201635]*. 


## Acknowledgement

To this repository is used, please cite: [todo]


## Steps
![Pipeline](https://github.com/mappingaeolus/mapping_medications/blob/master/pipeline1.png)
Starting from a table with a column identifying drugs the steps are:
1)  Extraction of the generic drug name
1)	Cleaning: cleaning from routes, concentration, percentage
2)	Exact matching: try to match exactly the drugs with the drugs in AEOLUS, for the ones for which did not work
3)	Fuzzy matching (threshold of 0.73): try to match allowing some errors, for the ones for which did not work
4)	Manual matching: the ones that did not match try to match them manually thanks to the collaboration with experts

## Repository structure

This repository is structured as follow:
- in Utils scripts and detailed instructions for running the pipeline
