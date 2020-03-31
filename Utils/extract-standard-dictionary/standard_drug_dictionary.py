#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 15:57:31 2020

"""

# Import Modules
import pandas as pd
import os
import sys
cwd = os.getcwd()
figs_path = cwd
sys.path.append(figs_path)

from constants_used_for_assemble_triples import *

import drug_cleaning_utils as dcu
import drug_cleaning_utils
import warnings
warnings.filterwarnings("ignore")


if __name__ == "__main__":
    print('Extracting clean and updated drug concepts from AEOLUS...')

    # AEOLUS files
    concept_drug_filtered = pd.read_csv(INPUT_FILES_DIR + 'concept_drug_filtered.csv')
    concept_drug_filtered_unique = concept_drug_filtered.drop_duplicates('concept_name')[['concept_name', 'concept_code', 'concept_id']]
    
    
    # Clean medications in the same way in both AEOLUS and extracted dataset
    aeolus_cleaned = dcu.clean_medications(concept_drug_filtered_unique, 'concept_name', unique =1)
    aeolus_cleaned.to_pickle(OUTPUT_FILES_DIR + 'aeolus_new_dictionary.pkl')
