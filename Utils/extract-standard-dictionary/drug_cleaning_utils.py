#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 14:38:11 2020

This file contains all useful functions for data cleaning 

"""


import pandas as pd
import jellyfish
import numpy as np
import re
from  more_itertools import unique_everseen

def match(value1, value2):
    "This function measures the Damerau Levenshtein distance between two medications"
    maximum = float(max(len(value1), len(value2)))
    distance = jellyfish.damerau_levenshtein_distance(value1, value2)
    return 1 - distance / maximum



def fuzzy_matching(max_distance, unique_meds, drug_dictionary):
    "This function applies fuzzy matching on a list of unique_meds and returns a dataframe of the results. Note that unique_meds must contain a binary column called in_aeolus"
    
    fuzzy_matching_results = {'generic_drug':[], 'concept_name_cleaned':[], 'score':[]}
    i=0
    print("Number of drugs to match = " + str(len(unique_meds.loc[unique_meds.in_aeolus==0])))
    for test_drug in unique_meds.loc[unique_meds.in_aeolus==0]['generic_drug']:
        print(i); i+=1
        df = drug_dictionary[['concept_name_cleaned']]
        df['len_diff'] =  (df['concept_name_cleaned'].str.len() - len(test_drug)).abs()
        df = df.loc[df.len_diff<=max_distance]
        df['results'] = df.apply(lambda x: match(test_drug, x["concept_name_cleaned"]), axis=1)
        if len(df)>0:
            df = df.reset_index()
            idx_max= df['results'].idxmax()
            best_match = df.iloc[idx_max]['concept_name_cleaned']
            score = df.iloc[idx_max]['results']
        else:
            best_match=np.nan
            score = np.nan
        fuzzy_matching_results['generic_drug']+=[test_drug]; fuzzy_matching_results['concept_name_cleaned']+=[best_match]
        fuzzy_matching_results['score']+=[score]
    df_fuzzy_matching_results = pd.DataFrame(fuzzy_matching_results)

    return df_fuzzy_matching_results

def replace(list_substrings, df, col):
    for s in list_substrings:
        df[col] = df[col].apply(lambda x: x.replace(s, ' '))
    return df


def clean_private(df, feat, unique=0):
    """ Cleaning steps specific to private dataset
    """
    new_column_name = feat + '_cleaned'
    df[new_column_name] = df[feat].apply(lambda x: x.lower().split(' (')[0])
    
    return df

def clean_medications(df, feat, unique =0):
    """df contains the medications to clean,
       feat is the string of the column name to clean,
       unique specifies if to return unique entries only 
       
       returns the cleaned dataframe with the new clean column as feat+'_cleaned'
    """
    new_column_name = feat + '_cleaned'
    
    if new_column_name in list(df):
        df[new_column_name] = df[new_column_name].apply(lambda x: x + ' ')
    else:
        df[new_column_name] = df[feat].str.lower().apply(lambda x: x + ' ')
        
    # Clean unnecessary separators and removes any text in brackets and digits/ dosages/ units
    df[new_column_name] = df[new_column_name].apply(lambda x: " ".join(filter(None, re.split(r'[\+\*\!\/\;\-\,\#\%\&\:\'\.\d+]\s*', x))))
    df[new_column_name] = df[new_column_name].apply(lambda x:  re.sub("[\(\[].*?[\)\]]", "", x))
    df = replace(['*', '\d+', 'mg', 'mg/', '.', ' ml ', ' nmol',' microgram ', 'milligram', 
                  'micrograms',' g ', ' ml ', 'mcg', ' gram' ,'mmol', 'units', ' kg'], df, new_column_name)
    df[new_column_name] = df[new_column_name].apply(lambda x: x + ' ')
    df = replace([ ' ltd)', ' # fda ', 'extended release', ' with ', ' fda ',' coated ', ' ud ', ')', 
                ' and ', ' sr ', ' hr ' , ' in ', ' hour ', 'delayed release', 'only',
                ' adhd ', 'notropics', ' agents', ' agent', ' used for', 'derivatives', 'related', 'drugs', 'drug', ' other ', 'combinations'
                , ' of ', ' or ', ' containing ', ' against ', ' used ', ' preparations', ' products', ' for ', ' other'], df, new_column_name) # spaces are necessary so we don't remove substrings of meds
    # Clean routes
    route = [' oral ', ' film coated ',  ' capsule ', ' solution ','chewable', ' injection ', ' gel ', ' injectable ', ' prefilled syringe ', ' iv push',
             ' sterile', ' val ', ' unit ', ' cap ', ' tab ', ' cp ', ' tb ', 'suspension' , ' iv ', 'human ', ' gel', 'iv bolus', ' vial', ' pen', 'transfuse', 
             'intravenous', 'syrup', ' original', ' jucy', ' creme', 'nasalspray', 'tablet', 'tablets', 'units', 'infusion', 'spray',
              ' split', ' inj', ' subq', ' susp', ' soln', ' kit', ' bolus', ' liquid', 'eye drops', ' nasal', 'phosphate', 'topical', 'sulf',
              'drops', 'syringe', ' drp','pre filled', 'continuous', 'drop', ' oph ', ' dose ', ' sulfate ', ' sodium ', ' ophth ', ' ophthalmic ', ' cream ', ' inhalator ', ' potassium ', ' caffeine ', ' codeine ', ' irrg ', ' ointment ', ' inhalation ', ' desensitization ', ' recombinant ', ' powder ', ' placebo ', ' preparation ', ' augmented ', ' xl ', ' capsules ', ' dextrose ', ' patch ']    
    df = replace(route, df, new_column_name)
    # Clean bases
    bases = [' nacl', ' hcl']
    df = replace(bases, df, new_column_name)
    # Diets
    diets = ['diet supplement', 'diet tube feed', 'diet supplement']
    df = replace(diets, df, new_column_name)
    # Get rid of spaces
    df[new_column_name] = df[new_column_name].apply(lambda x: ' '.join(unique_everseen(x.split())))
    if unique == 1:
        df = df.drop_duplicates(new_column_name)
    return df
