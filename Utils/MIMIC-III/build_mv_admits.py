#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 19:18:37 2019

@author: rick
"""

import sys


from constants_used_for_assemble_triples import *

import psycopg2
import psycopg2.extras

def drop__mv_admits__table(db):
    print('\nEntered drop__mv_admits__table')
    q = " DROP TABLE mv_admits "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table mv_admits')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table mv_admits did not exist')

def create_and_populate__mv_admits__table(db):
    print('\nEntered create_and_populate__mv_admits__table')
    print('   (This takes 2 or 3 minutes)')
    # this is the query from Azza; should yield 23049 records
    q = """
         create table mv_admits as (
	       select hadm_id from ((
		     (select distinct hadm_id from admissions)
		     except (select distinct hadm_id from chartevents c, d_items i where
				i.itemid = c.itemid
				and dbsource = 'carevue'))
		     except (select distinct hadm_id from inputevents_cv)) as t)
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Created table mv_admits, including a commit')
    
    
def add_index_to__mv_admits(db):
    print('\nEntered add_index_to__mv_admits')
    q = """
          create index mva__hadm_id
          on mv_admits(hadm_id)
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Added index to mv_admits, including a commit')

def create_and_build__mv_admits__table(db):
    print('\nEntered create_and_build__mv_admits__table')
    
    drop__mv_admits__table(db)
    create_and_populate__mv_admits__table(db)
    add_index_to__mv_admits(db)
        

####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    
	print('Build z_prescriptions_mv table')
	
	# open postgres connection with mimic database
    db = connect_postgres()
	
    #add_indexes_to_mimiciii_tables(db)
    build_mv_prescriptions_table.create_and_build__z_prescriptions_mv__table(db)
	
	
	# close connection to the mimic database    
    close_postgres(db)