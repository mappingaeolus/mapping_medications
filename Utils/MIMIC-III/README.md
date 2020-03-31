In this folder you will find the script that will create the tables from MIMIC-III to start the cleaning and the matching with AEOLUS.

The files must be runned in this order:
- build_mv_admits.py
- build_mv_prescriptions_table.py

Before running the scripts you must change in the constants_used_for_assemble_triples.py file the credential to access to your MIMIC database.

In the manual_matching_files folder you can find the csv files that contains the manual matching between AEOLUS and some of the cleaned MIMIC drugs.In this file, the initially unmatched medications were manually matched using the active ingredients found in DrugBank version 5.1.5. The manual matching took into considerations the dosages and units that varies between the two databases, and matched to the most general active ingredients in Aeolus from MIMIC-III if no exact dosage were present. 


