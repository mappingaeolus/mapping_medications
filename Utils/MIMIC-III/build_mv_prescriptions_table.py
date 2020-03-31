#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on March 22 2020

@author: Elisa and rick
"""


import sys


import psycopg2
import psycopg2.extras

from constants_used_for_assemble_triples import *

from datetime import datetime

def drop__z_prescriptions_mv__table(db):
    print('\nEntered drop__z_prescriptions_mv__table')
    q = " DROP TABLE z_prescriptions_mv "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table z_prescriptions_mv')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table z_prescriptions_mv did not exist')

def create_populate_clean_index__z_prescriptions_mv__table(db):
    print('\nEntered create_populate_clean_index__z_prescriptions_mv__table')
    
    '''
    high level intuition: 
    based on
      a) having a drug, drug_name_poe, drug_name_generic in prescriptions
          somewhere that matches the NULL-gsn row
      b) if there are > 1 gsn values associated to this triple,
          take the gsn value that has highest occurrance count
	      for this triple
    '''
    q = """
	      create table z_prescriptions_mv as
	      select *
	      from prescriptions
	      where startdate is not null
		    and hadm_id in (select hadm_id from mv_admits);
                    
	      update z_prescriptions_mv
	      set drug = left(drug, -1)
	      where drug like '% ';

	      update z_prescriptions_mv
	      set drug_name_poe = left(drug_name_poe, -1)
	      where drug_name_poe like '% ';

	      update z_prescriptions_mv
	      set drug_name_generic = left(drug_name_generic, -1)
	      where drug_name_generic like '% ';

	      update z_prescriptions_mv
	      set drug = right(drug, -1)
	      where drug like ' %';

	      update z_prescriptions_mv
	      set drug_name_poe = right(drug_name_poe, -1)
	      where drug_name_poe like ' %';

	      update z_prescriptions_mv
	      set drug_name_generic = right(drug_name_generic, -1)
	      where drug_name_generic like ' %';
          
	      create index z_prescriptions_mv__drug
	      on z_prescriptions_mv(drug);

	      create index z_prescriptions_mv__gsn
	      on z_prescriptions_mv(gsn);

	      create index z_prescriptions_mv__drug_gsn
	      on z_prescriptions_mv(drug, gsn);

	      create index z_prescriptions_mv__drug_drugtype_gsn
	      on z_prescriptions_mv(drug, drug_type, gsn);

	      create index z_prescriptions_mv__subject_id
	      on z_prescriptions_mv(subject_id);

	      create index z_prescriptions_mv__hamd_id
	      on z_prescriptions_mv(hadm_id);

	      create index z_prescriptions_mv__icustay_id
	      on z_prescriptions_mv(icustay_id);
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Created, populated, cleaned, and indexed table z_prescriptions_mv, including a commit')
    
    
    
##############################################################################
#
#  assembling z_presciptions_mv
#
##############################################################################

    
def create_and_build__z_prescriptions_mv__table(db):
    print('\nEntered create_and_build__z_prescrip_drug_quadruples_with_normalized_gsnids__table')
    
    drop__z_prescriptions_mv__table(db)
    create_populate_clean_index__z_prescriptions_mv__table(db)
    
    print('\nCompleted execution of create_and_build__z_prescrip_drug_quadruples_with_normalized_gsnids__table')


####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    pass