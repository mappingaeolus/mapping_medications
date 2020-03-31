#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  4 20:15:49 2020

Updated on Tuesday March 24, 2020
"""

# Import Modules
import pandas as pd
import numpy as np
import os
import sys
cwd = os.getcwd()
figs_path = cwd
sys.path.append(figs_path)

from constants_used_for_assemble_triples import *

import drug_cleaning_utils as dcu
import warnings
warnings.filterwarnings("ignore")




if __name__ == "__main__":
    print('Importing raw data... ')
	
    # input a pickle file with the medications to be mapped called drug.pkl
    df_meds_raw =  pd.read_pickle(INPUT_FILES_DIR + 'drug.pkl') 
    df_meds_raw = df_meds_raw.loc[df_meds_raw.Medication.notna()]

	# insert a pickle file with the drugs coming from AEOLUS
    aeolus_cleaned = pd.read_pickle(OUTPUT_FILES_DIR + 'aeolus.pkl')
    
    print('Cleaning raw data... ')
    df_meds_cleaned = dcu.clean_private(df_meds_raw, 'Medication', unique=0)
    df_meds_cleaned = dcu.clean_medications(df_meds_raw, 'Medication', unique=0)
    df_unique_meds = df_meds_cleaned.drop_duplicates('Medication_cleaned').rename(columns={'Medication_cleaned': 'generic_drug'})

    print('Mapping cleaned data to AEOLUS based on exact matching...')
    # [1] Check if generic drug is in AEOLUS and extract generic drug as AEOLUS
    df_unique_meds['in_aeolus'] = np.nan
    df_unique_meds['in_aeolus'] = np.where(df_unique_meds['generic_drug'].isin(aeolus_cleaned.concept_name_cleaned.str.lower().unique()), 1, 0)
    df_unique_meds['aeolus'] = np.where(df_unique_meds['in_aeolus']==1, df_unique_meds['generic_drug'], np.nan)

    print('Total exact matches in AEOLUS = ' + str(df_unique_meds.in_aeolus.sum()))

    print('Fuzzy matching... ')
    # [2] Fuzzy matching based on maximum distance of 10
    df_fuzzy_matching_results = dcu.fuzzy_matching(10, df_unique_meds, aeolus_cleaned)
   
    print('Storing unmatched drugs (score<0.73)')
    # Get results with low score 
    unmatched_drugs = df_fuzzy_matching_results.loc[df_fuzzy_matching_results.score<=0.73].generic_drug.values
    unmatched_drugs=pd.DataFrame(unmatched_drugs)
	# store drug that do not match with AEOLUS
    unmatched_drugs.to_pickle(OUTPUT_FILES_DIR + './unmatched_drug.pkl')
    
    print('Linking meds to AEOLUS matching...')
    # [3] Link ccad_meds_cleaned to AEOLUS matching 
    # Append exact matchings and fuzzy matching results 
    matching_dict = pd.concat([df_meds_cleaned.loc[df_unique_meds.in_aeolus==1][['generic_drug', 'aeolus']], 
                               df_fuzzy_matching_results.loc[df_fuzzy_matching_results.score>=0.73].rename(columns={'concept_name_cleaned':'aeolus'})[['generic_drug', 'aeolus']]])
    
    df_meds_cleaned = df_meds_cleaned.merge(matching_dict, left_on='Medication_cleaned', right_on='generic_drug', how='left')
	# store final matching between input medications, cleaned medications and AEOLUS drugs
	# change the name of the output file to give in input to manual_matching.py
    df_meds_cleaned.to_pickle(OUTPUT_FILES_DIR + 'final_meds.pkl')

	

