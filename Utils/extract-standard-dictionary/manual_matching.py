#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 29 2019
"""

import sys


import psycopg2
import psycopg2.extras

import jellyfish
import pandas as pd
import numpy as np

from constants_used_for_assemble_triples import *

from datetime import datetime

#####################################################
#
#  connection to the database
#     to modify the credential of the database see 
#     the constants_used_for_assemble_triples.py file
#
#####################################################

def connect_postgres():
    try:
        print('\nAttempting to connect to the MIMIC database')
        conn = psycopg2.connect(POSTGRES_SIGN_IN_CREDENTIALS)
        cursor = conn.cursor()
        print('Successfully connected to the MIMIC database, and created a cursor for it')
        db = {}
        db['conn'] = conn
        db['cursor'] = cursor
        # set search path to mimiciii database
        cursor.execute("set search_path to mimiciii;")
        conn.commit()
        print('Set search_path to "mimiciii" and committed')
        return db
    except:
        print ("\nI am unable to connect to the database")
        print('\nExiting program because failed to connect to mimic database\n')
        sys.exit()

def close_postgres(db):
    try:
        db['cursor'].close()
        db['conn'].close()
        print('\nHave closed the cursor and connection to the MIMIC database')
    except:
        print('\nInput to the close_postgres function call is not holding a postgres connection and cursor')
	

#----------------------------------------------------
#    create the t_manual_matching_aeolus 
#      tables for inputevents and prescriptions
#----------------------------------------------------
def drop__t_manual_matching_inputevents_aeolus__table(db):
    print('\nEntered drop__t_manual_matching_aeolus__table')
    q = " DROP TABLE t_manual_matching_inputevents_aeolus "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table t_manual_matching_inputevents_aeolus')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table t_manual_matching_inputevents_aeolus did not exist')
		
def create__t_manual_matching_inputevents_aeolus__table(db):
    print('\nEntered create__t_manual_matching_aeolus__table')
    q = """
          CREATE TABLE t_manual_matching_inputevents_aeolus (
             mimic_drug varchar,
             manual_drug varchar
          )
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Created table t_manual_matching_inputevents_aeolus, including a commit')
 
def import_csv_into__t_manual_matching_inputevents__table(db):
    print('\nEntered import_csv_into__t_manual_matching_aeolus_i__table')
    q = """
           copy t_manual_matching_inputevents_aeolus 
                   (mimic_drug, manual_drug)
           from stdin with
           delimiter as ','
		   csv
		   header
        """
    fileName = 'inputevents_mimic_aeolus_manual_matching.csv'
    
    f = open(AEOLUS_MANUAL_MATCHING_FILES_DIR + fileName, 'r')
    db['cursor'].copy_expert(sql=q, file=f)
    f.close()
    db['conn'].commit()
	

	
def drop__t_manual_matching_prescriptions_aeolus__table(db):
    print('\nEntered drop__t_manual_matching_prescriptions_aeolus__table')
    q = " DROP TABLE t_manual_matching_prescriptions_aeolus "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table t_manual_matching_prescriptions_aeolus')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table t_manual_matching_prescriptions_aeolus did not exist')
		
def create__t_manual_matching_prescriptions_aeolus__table(db):
    print('\nEntered create__t_manual_matching_prescriptions_aeolus__table')
    q = """
          CREATE TABLE t_manual_matching_prescriptions_aeolus (
             mimic_drug varchar,
             manual_drug varchar
          )
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Created table t_manual_matching_prescriptions_aeolus, including a commit')
 

def import_csv_into__t_manual_matching_prescriptions_aeolus__table(db):
    print('\nEntered import_csv_into__t_manual_matching_prescriptions_aeolus__table')
    q = """
           copy t_manual_matching_prescriptions_aeolus 
                   (mimic_drug, manual_drug)
           from stdin with
           delimiter as ','
		   csv
		   header
        """	
    fileName = 'prescriptions_mimic_aeolus_manual_matching.csv'
    
    f = open(fileName, 'r')
    db['cursor'].copy_expert(sql=q, file=f)
    f.close()
    db['conn'].commit()
	
	
####################################################
#
#  working to create the matching_inputevents_aeolus
#       table that will link drugs from inputevents 
#       mimic table with drugs from AEOLUS
#
#####################################################

#----------------------------------------------------
#    create the matching_inputevents_aeolus table
#----------------------------------------------------
def drop__matching_inputevents_aeolus__table(db):
    print('\nEntered drop__matching_inputevents_aeolus__table')
    q = " DROP TABLE matching_inputevents_aeolus"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table drop__matching_inputevents_aeolus__table')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table drop__matching_inputevents_aeolus__table did not exist')

def create__matching_inputevents_aeolus__table(db):
    print('\nEntered create__matching_inputevents_aeolus__table')  
    q = """
	      CREATE TABLE matching_inputevents_aeolus(
		  id int4,
		  medication varchar(200),
		  medication_cleaned varchar(200),
		  generic_drug varchar(200),
		  aeolus varchar(200),
		  aeolus_m int4,
		  manual int4
		  );
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Created table matching_inputevents_aeolus, including a commit')
	
def populate__matching_inputevents_aeolus__table(db):
    print('\nEntered populate___matching_inputevents_aeolus__table')
	# meds_final_inputevents.pkl is the file of output of ccad_map_aeolus.py given in input drugs from inputevents
    inputevents_med = pd.read_pickle(OUTPUT_FILES_DIR + 'meds_final_inputevents.pkl')
    inputevents_med.to_csv('inputevents_med.csv')
    q = """
           copy matching_inputevents_aeolus 
                   (id, medication, medication_cleaned, generic_drug, aeolus)
           from stdin with
           delimiter as ','
		   csv
		   header
        """	
    fileName = 'inputevents_med.csv'
    f = open(AEOLUS_MANUAL_MATCHING_FILES_DIR + fileName, 'r')
    db['cursor'].copy_expert(sql=q, file=f)
    f.close()
    print(type(inputevents_med))
    print('populate table matching_inputevents_aeolus, including a commit')

#-------------------------------------------------------------
#    apply the manual matching btw drug_cleaned and drugs from
#    AEOLUS taking data from t_manual_matching_inputevents_aeolus table
#-------------------------------------------------------------
def update_matching_inputevents_aeolus_with_drugs_from__t_manual_matching_aeolus__table(db):
	print('\nEntered update_drugs_aeolus_matching_with_ingredients_from__t_manual_matching_aeolus__table')
	q = """
		  update matching_inputevents_aeolus set manual=0;
		  update matching_inputevents_aeolus set aeolus_m=0;
		  update matching_inputevents_aeolus set aeolus_m=1 where aeolus is not null;
			
		  update matching_inputevents_aeolus as m
		  set aeolus = nv.manual_drug, manual=1
		  from t_manual_matching_inputevents_aeolus nv
		  where m.medication_cleaned=nv.mimic_drug and m.aeolus is null;
		
		"""
	db['cursor'].execute(q)
	db['conn'].commit()
	print('update the drug_aeolus field in matching_inputevents_aeolus and matching_prescriptions_aeolus from t_manual_matching_aeolus table, including a commit')
	
#----------------------------------------------------
#    create the matching_prescriptions_aeolus table
#----------------------------------------------------
def drop__matching_prescriptions_aeolus__table(db):
    print('\nEntered drop__matching_prescriptions_aeolus__table')
    q = " DROP TABLE matching_prescriptions_aeolus"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table drop__matching_prescriptions_aeolus__table')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table drop__matching_prescriptions_aeolus__table did not exist')

def create__matching_prescriptions_aeolus__table(db):
    print('\nEntered create__matching_prescriptions_aeolus__table')    
    q = """
	      CREATE TABLE matching_prescriptions_aeolus(
		  id int4,
		  medication varchar(200),
		  medication_cleaned varchar(200),
		  generic_drug varchar(200),
		  aeolus varchar(200),
		  aeolus_m int4,
		  manual int4
		  );
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Created table matching_prescriptions_aeolus, including a commit')
	
def populate__matching_prescriptions_aeolus__table(db):
    print('\nEntered populate___matching_prescriptions_aeolus__table')
    inputevents_med = pd.read_pickle(OUTPUT_FILES_DIR + 'meds_final_prescriptions.pkl')
    inputevents_med.to_csv('prescriptions_med.csv')
    q = """
           copy matching_prescriptions_aeolus 
                   (id, medication, medication_cleaned, generic_drug, aeolus)
           from stdin with
           delimiter as ','
		   csv
		   header
        """	
    fileName = 'prescriptions_med.csv'
    f = open(fileName, 'r')
    db['cursor'].copy_expert(sql=q, file=f)
    f.close()
    print(type(inputevents_med))
    print('populate table matching_prescriptions_aeolus, including a commit')

def update_matching_prescriptions_aeolus_with_drugs_from__t_manual_matching_aeolus__table(db):
	print('\nEntered update_drugs_aeolus_matching_with_ingredients_from__t_manual_matching_aeolus__table')		
	q = """
		  update matching_prescriptions_aeolus set manual=0;
		  update matching_prescriptions_aeolus set aeolus_m=0;
		  update matching_prescriptions_aeolus set aeolus_m=1 where aeolus is not null;
		  
		  update matching_prescriptions_aeolus as m
		  set aeolus = nv.manual_drug, manual=1
		  from t_manual_matching_prescriptions_aeolus nv
		  where m.medication_cleaned=nv.mimic_drug and m.aeolus is null;		
		"""
	db['cursor'].execute(q)
	db['conn'].commit()
	print('update the drug_aeolus field in matching_inputevents_aeolus and matching_prescriptions_aeolus from t_manual_matching_aeolus table, including a commit')
	
	
#####################################################
#
#      if medication_cleaned do not match automatically 
#      or manually with AEOLUS set medication_cleaned as
#      as the final one both for inputevents and prescriptions
#
#####################################################
def final_update_matching_inputevents_aeolus__table(db):
	print('\nEntered final_update_matching_inputevents_aeolus__table')
	q = """
		  UPDATE matching_inputevents_aeolus 
		  SET aeolus = medication_cleaned
		  where aeolus is null;		
		"""
	db['cursor'].execute(q)
	db['conn'].commit()
	print('update the drug_aeolus field for final matching, including a commit')

def final_update_matching_prescriptions_aeolus__table(db):
	print('\nEntered final_update_matching_prescriptions_aeolus__table')		
	q = """
		  UPDATE matching_prescriptions_aeolus 
		  SET aeolus = medication_cleaned
		  where aeolus is null;
		
		"""
	db['cursor'].execute(q)
	db['conn'].commit()
	print('update the drug_aeolus field for final matching, including a commit')

if __name__ == "__main__":
    print('Manual matching from prescriptions_mv and inputevents_mv...')
	
    db = connect_postgres()

	# insert manual matching with AEOLUS data
    drop__t_manual_matching_inputevents_aeolus__table(db)
    create__t_manual_matching_inputevents_aeolus__table(db)
    import_csv_into__t_manual_matching_inputevents__table(db)
	
    drop__t_manual_matching_prescriptions_aeolus__table(db)
    create__t_manual_matching_prescriptions_aeolus__table(db)
    import_csv_into__t_manual_matching_prescriptions_aeolus__table(db)

	# create the mapping table
    drop__matching_inputevents_aeolus__table(db)
    create__matching_inputevents_aeolus__table(db)
    populate__matching_inputevents_aeolus__table(db)
	
    drop__matching_prescriptions_aeolus__table(db)
    create__matching_prescriptions_aeolus__table(db)
    populate__matching_prescriptions_aeolus__table(db)
	
    update_matching_inputevents_aeolus_with_drugs_from__t_manual_matching_aeolus__table(db)
    update_matching_prescriptions_aeolus_with_drugs_from__t_manual_matching_aeolus__table(db)

	# final update if not matching with AEOLUS
    final_update_matching_inputevents_aeolus__table(db)
    final_update_matching_prescriptions_aeolus__table(db)
	
    close_postgres(db)
