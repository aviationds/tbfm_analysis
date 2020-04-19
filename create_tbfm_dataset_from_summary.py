##@Copyright 2020. Ulrich Linder, Al Capps
##@Organization - aviationdatascience.org non-profit, through release from Penn State
##@License - This work is licensed under MIT license - https://opensource.org/licenses/MIT
## This permissive license lets you do basically whatever you want.
## Per the details of the MIT license, the software is provided "as is".
## Have fun!

## In support of Penn State Data Science, this is the last step in data parsing 
##of publicly available TBFM SWIM data
## This program starts with the daily TBFM summary and then
## winnows down the data into a dataset that was used for machine learning studies
### This is provided primarily as an example to make it easier for folks to 
##  create their own sub-sets for studies
import pandas as pd
import os
import argparse
import os.path
import time
import numpy as np

nat = np.datetime64('NaT')

## Main loop###
def main():
 
    out_dir=parms["outdir"]
    full_path=parms["readFile"]
    print("Processing "+full_path,flush=True)
    start = time.time()

    messages_pd = pd.read_csv(full_path)

    ##Get all the APREQs and print
    tbfm_apreqs=messages_pd[pd.notnull(messages_pd['std'])]
    
    filename=os.path.basename(full_path)
    nameparts=filename.split("_")
    outfile=out_dir + "/" + nameparts[0] + "_all_tbfm_apreqs.csv"
    tbfm_apreqs.to_csv(outfile,index=False)
    
    ## Get rid of overflights. Useful if studying meter point demand
    ## but not arrival metering or EDC delay passback to a flight
    messages_pd=messages_pd[(messages_pd.cat != "OVERFLIGHT")]

    ## (Likely) Arrivals with at least one ETA to runway
    ## This may be over-simplifyng the search for arrival TBFM flights,
    ## but seems to do a pretty good job and includes departures
    ## that are scheduled into arrival metering
   
    tbfm_arrs =messages_pd[pd.notnull(messages_pd.eta_rwy)]
           
    ## For the purposes of a cleaner look at flights that TBFM
    ## thinks completed their arrival (and landed), the list is
    ## further winnowed down. Others may not want this.
    ## Also ensuring we have the TBFM runway.
    ## Note: on check was missing about 5% of runways for arrivals 
    tbfm_arrs =tbfm_arrs[(tbfm_arrs.acs == "LANDED") &
                         pd.notnull(tbfm_arrs.rwy)]      
    
    outfile=out_dir + "/" + nameparts[0] + "_all_tbfm_arrivals.csv"
    tbfm_arrs.to_csv(outfile,index=False)

    ### Filter on a single airport here - if needed ####
    
    stop = time.time()
    duration = (stop - start)/60
    print("Processing took "+ str(duration) + " minutes",flush=True)
        
        
        
def build_parms(args):
    """Helper function to parse command line arguments into dictionary
        input: ArgumentParser class object
        output: dictionary of expected parameters
    """
    readFile=args.file
    outdir=args.outdir         
    parms = {"readFile":readFile,
             "outdir":outdir}                   
    return(parms)
        

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = \
                 "Create Dataset.")
    parser.add_argument("file", 
                        help = "Script to help create TBFM dataset.")
    parser.add_argument("--outdir", default = "./")
    parms = build_parms(parser.parse_args())
    main()
