##@Copyright 2020. Ulrich Linder, Al Capps
##@Organization - aviationdatascience.org non-profit through release from Penn State
##@License - This work is licensed under MIT license - https://opensource.org/licenses/MIT
## This permissive license lets you do basically whatever you want.
## Per the details of the MIT license, the software is provided "as is".
## Have fun!

## In support of Penn State Data Science, this is the 2nd step in data parsing 
##of publicly available TBFM SWIM data
## This program beings with the flattened output of earlier "air" messages in CSV 
## format and stores the latest information in a single record that is
## output to a CSV file. This output becomes an easy way to look up
## all the flights that TBFM thinks flew a particular day as well as its
## unique identifiers. This output is often then used as a starting point
## when iterating back through all flattened CSV TBFM messages for that day.

import gzip
import os
import re
import argparse
import os.path
import time
import pandas as pd
from datetime import datetime
from datetime import timedelta 


## Main loop###
## searches the directory given, processes each TBFM AIR CSV file
## stores the data in a python dictionary (with inner class) 
## then once complete with the last file, reconstructs a record of
## each flight with as complete a dataset as possible
## for each snapshot of specified duration. Note: originally this was done
## in a pandas dataframe but the processing was slow so this approach was taken 
def main():
 
    start = time.time()
    tbfmFlights={}   
    ldir=os.listdir(parms["readDir"])
    path=os.getcwd() + "\\" + parms["readDir"]

    out_dir=parms["outdir"]

    
    #20191102 format as string
    ###For this analysis, the local day starts at 0700 and
    ### and ends the next zulu date with teh end of the 0600 file
    ### One major reason for this is that end users generally like to see
    ### the results in local time. Another reason is this split leads to the
    ### fewest missing messages between days (given lowest demand)
    target_date=parms["target_date"]
    dt_target = datetime.strptime(target_date, "%Y%m%d")
    dt_nextday = dt_target + timedelta(days=1)
    string_nextday=dt_nextday.strftime("%Y%m%d") 
    files_to_process=create_filelist(ldir,target_date,string_nextday)


    for filename in files_to_process:
        ## Only process files that are  AIR CSV files
        if not filename.endswith("air.csv.gz"):
            continue
        
        full_path=path + "\\" + filename
        print("Processing "+full_path,flush=True)
        
        ## iterate through all messages
        ## match on aid, tmaId, dap, apt and update internal info 
        ## to keep the latest info on each flight
        with gzip.open(full_path,'rt') as f:
         for line in f:   
            str_line=str(line).rstrip()
            #print(str_line)
            result=re.split(',',str_line)
            
            ### Use the header to create our column key-value names
            if "msgtime" in result[0]:
                columnames=result
                continue
            msgtime=result[0]
            aid=result[1]
            tmaId=result[2]
            dap=result[3]
            apt=result[4]
            
            if aid and tmaId and dap and apt:
                pass
            else:
                #The elements for a key do not exist. Skipping.
                continue
            
            key="aid=" + aid +",tmaId=" +tmaId + ",dap=" + dap + ",apt=" + apt

            ##Add or retrieve unique flight, update relevant items
            ## Each populated item is updated as a key/value pair that 
            ## is stored in an instantiated innner class. This adds
            ##complexity but was done primarily for high efficiency.
            if not key in tbfmFlights:
              ##Create a blank TflightListDict class to store key specific info
              value=TflightListDict(key)
              for entry in range(len(result)):
                  if not result[entry] in (None,""):
                      #Always update with the latest
                      value.dictlist[entry]={columnames[entry] : result[entry] }
                  
                  #Given this is the first message for this flight, denote it
                  value.dictlist[56]={'firstmtime' : msgtime }
                  
                  ##Unfortunately hard coded, entry 38 is 'std' element
                  if (entry == 38):
                      thisSTD=result[entry]
                      if (len(thisSTD) > 0):
                          value=update_std_elements(value,key,thisSTD,'new',msgtime)                       
                      
                  tbfmFlights.update({key : value})
            else:
               ##We already have an entry for this flight. Update it.
               value= tbfmFlights.get(key)
               for entry in range(len(result)):
                  if not result[entry] in (None,""):                    
                      ##If we have an STD/APREQ, special logic to add other elements
                      if (columnames[entry] == 'std'):
                          thisSTD=result[entry]
                          if (len(thisSTD) > 0):
                            value=update_std_elements(value,key,thisSTD,'update',msgtime)
                          
                      #Always update with the latest
                      value.dictlist[entry]={columnames[entry] : result[entry] }
                                           
    outname=out_dir + "/"+target_date+"_tbfm_swim_flightsummary_AIR_out.csv"                            
    printFlights(tbfmFlights,outname,columnames)
            
    stop = time.time()
    duration = (stop - start)/60
    print("Processing took "+ str(duration) + " minutes",flush=True)

##A helper method to reduce the complexity of the main processing loop
def update_std_elements(value,key,thisSTD,status,msgtime):
            
    if(status == "new"):
        #This is a new flight being added, set orig/latest APREQ time
        #print("first std when creating:"+key +":"+thisSTD)
        value.dictlist[58]={'firststdtime':msgtime}
        value.dictlist[57]={'laststdtime':msgtime}
        diff = compute_time_diff_secs(thisSTD,msgtime)
        value.dictlist[59]={'stdminusnowtime':diff}
        
    elif (status =="update"):
        ##We have a flight but don't know at this point
        ##if this is really an update to std, new add, or duplicate
        ##First, we get the currently stored value for 'std' for this flight
        current_std=value.dictlist[38].get('std',"")
                        
        if (len(current_std) ==0):
            ##We do not currently have an 'std' so we add it
            value.dictlist[58]={'firststdtime':msgtime}
            value.dictlist[57]={'laststdtime':msgtime}
            diff = compute_time_diff_secs(thisSTD,msgtime)
            value.dictlist[59]={'stdminusnowtime':diff}
            #print("This is the first std when updating flight:"+key)
        else:
            ##We have an 'std' already. Is it really new or duplicate?
            if (current_std == thisSTD):
                #duplicate STD, do nothing
                pass
            else:
                #print("updated STD from "+current_std +" to "+thisSTD)
                value.dictlist[57]={'laststdtime':msgtime}
                diff = compute_time_diff_secs(thisSTD,msgtime)
                value.dictlist[59]={'stdminusnowtime':diff}
                num_updates=value.dictlist[60].get('numstdupdates',"")
                num_updates = num_updates +1
                value.dictlist[60]={'numstdupdates':num_updates}

    return value
    
##Method to return the difference between two times, in seconds
##Inputs: two strings that have format like 2019-11-01T07:50:52.794Z
def compute_time_diff_secs(firsttime,secondtime):           
            if(pd.isna(firsttime)):
                return

            if(pd.isna(secondtime)):
                return
            
            firsttime_obj=datetime.strptime(firsttime,'%Y-%m-%dT%H:%M:%SZ')
            secondtime_obj=datetime.strptime(secondtime,'%Y-%m-%dT%H:%M:%S.%fZ')
            # Convert to Unix timestamp
            d1_ts = time.mktime(firsttime_obj.timetuple())
            d2_ts = time.mktime(secondtime_obj.timetuple())

            secs_diff = int(d1_ts-d2_ts) 
            return (secs_diff) 
        
def create_filelist(ldir,target_date,string_nextday):
    filelist=[]

    ## Look for files from target date first    
    for hour in range(7,24):
        hour_str=str('{:02d}'.format(hour))
        file=target_date+"_"+hour_str+"00"
        #print(file)
        for filename in ldir:
            if (file in  filename) and (filename.endswith("air.csv.gz")):
                #print(filename)
                filelist.append(filename)
    ## Now same thing for 6 files from next day
    for hour in range(0,7):
        hour_str=str('{:02d}'.format(hour))
        file=string_nextday+"_"+hour_str+"00"
        #print(file)
        for filename in ldir:
            if (file in  filename) and (filename.endswith("air.csv.gz")):
                #print(filename)
                filelist.append(filename)
    
    list_len=len(filelist)
    if (list_len < 23):
        print("Missing a file.")
        print(filelist)
        exit()

    return filelist
        
### Iterates through each flight in the dictionary and prints the 
### information in a very specific manner. Storing is in generic key/value
### pairs but printing is hard coded. There is probably a more elegant way
### to do this but this worked nicely for this team's needs
def printFlights(tbfmFlights,outfile,columnames):

        outF = open(outfile, "w")
        outF.write("lastmsgtime,aid,tmaId,"+
        "dap,apt,mfx,cat,gat,bcn,rwy,scn,fps,"+
        "acs,typ,eng,spd,trw,sfz,rfz,eta_rwy,eta_mfx,"+
        "eta_oma,eta_sfx,eta_dfx,eta_o4a,eta_o3a,"+
        "eta_ooa,sta_o4a,sta_o3a,sta_ooa,sta_oma,"+
        "sta_dfx,sta_sfx,ara,tds,cfx,ctm,etd,std,"+
        "etm,est,a10,tcr,dfx,sfx,oma,ooa,o3a,o4a,"+
        "ina,sus,man,sta_rwy,sta_mfx,cfg,tra,firstmsgtime,laststdtime,"+
        "firststdtime,stdminusnowtime,numstdupdates\n")
        
        for key, value in tbfmFlights.items():
            num_apreqs=str(value.dictlist[60].get('numstdupdates',0))
            stdvsnow=str(value.dictlist[59].get('stdminusnowtime',-999))
            
            outF.write(value.dictlist[0].get('msgtime',"")+","+
            value.dictlist[1].get('aid',"")+","+
            value.dictlist[2].get('tmaId',"")+","+
            value.dictlist[3].get('dap',"")+","+
            value.dictlist[4].get('apt',"")+","+
            value.dictlist[5].get('mfx',"")+","+
            value.dictlist[6].get('cat',"")+","+
            value.dictlist[7].get('gat',"")+","+
            value.dictlist[8].get('bcn',"")+","+
            value.dictlist[9].get('rwy',"")+","+
            value.dictlist[10].get('scn',"")+","+
            value.dictlist[11].get('fps',"")+","+
            value.dictlist[12].get('acs',"")+","+
            value.dictlist[13].get('typ',"")+","+
            value.dictlist[14].get('eng',"")+","+
            value.dictlist[15].get('spd',"")+","+
            value.dictlist[16].get('trw',"")+","+
            value.dictlist[17].get('sfz',"")+","+
            value.dictlist[18].get('rfz',"")+","+
            value.dictlist[19].get('eta_rwy',"")+","+	
            value.dictlist[20].get('eta_mfx',"")+","+
            value.dictlist[21].get('eta_oma',"")+","+
            value.dictlist[22].get('eta_sfx',"")+","+
            value.dictlist[23].get('eta_dfx',"")+","+
            value.dictlist[24].get('eta_o4a',"")+","+
            value.dictlist[25].get('eta_o3a',"")+","+
            value.dictlist[26].get('eta_ooa',"")+","+
            value.dictlist[27].get('sta_o4a',"")+","+
            value.dictlist[28].get('sta_o3a',"")+","+
            value.dictlist[29].get('sta_ooa',"")+","+
            value.dictlist[30].get('sta_oma',"")+","+
            value.dictlist[31].get('sta_dfx',"")+","+
            value.dictlist[32].get('sta_sfx',"")+","+
            value.dictlist[33].get('ara',"")+","+
            value.dictlist[34].get('tds',"")+","+
            value.dictlist[35].get('cfx',"")+","+
            value.dictlist[36].get('ctm',"")+","+
            value.dictlist[37].get('etd',"")+","+
            value.dictlist[38].get('std',"")+","+
            value.dictlist[39].get('etm',"")+","+
            value.dictlist[40].get('est',"")+","+
            value.dictlist[41].get('a10',"")+","+
            value.dictlist[42].get('tcr',"")+","+
            value.dictlist[43].get('dfx',"")+","+
            value.dictlist[44].get('sfx',"")+","+
            value.dictlist[45].get('oma',"")+","+
            value.dictlist[46].get('ooa',"")+","+
            value.dictlist[47].get('o3a',"")+","+
            value.dictlist[48].get('o4a',"")+","+
            value.dictlist[49].get('ina',"")+","+
            value.dictlist[50].get('sus',"")+","+
            value.dictlist[51].get('man',"")+","+
            value.dictlist[52].get('sta_rwy',"")+","+
            value.dictlist[53].get('sta_mfx',"")+","+
            value.dictlist[54].get('cfg',"")+","+
            value.dictlist[55].get('tra',"")+","+
            value.dictlist[56].get('firstmtime',"")+","+
            value.dictlist[57].get('laststdtime',"")+","+
            value.dictlist[58].get('firststdtime',"")+","+
            stdvsnow+","+
            num_apreqs+"\n"
            )
            
        outF.close()

## Class used to allow updating of name value pairs as individual entries
## are updated. This structure can be expanded to include derived elements
## that would be part of the daily summary (e.g. - first frozen, time of APREQ)
class TflightListDict:
    def __init__(self, gufi):
        self.dictlist = [       
        {'msgtime' : ""}, 
        {'aid' : ""},
        {'tmaId' :""},
        {'dap':""},
        {'apt':""},
        {'mfx':""},
        {'cat':""},
        {'gat':""},
        {'bcn':""},
        {'rwy':""},
        {'scn':""},
        {'fps':""},
        {'acs':""},
        {'typ':""},
        {'eng':""},
        {'spd':""},
        {'trw':""},
        {'sfz':""},
        {'rfz':""},
        {'eta_rwy':""},	
        {'eta_mfx':""},
        {'eta_oma':""},
        {'eta_sfx':""},
        {'eta_dfx':""},
        {'eta_o4a':""},
        {'eta_o3a':""},
        {'eta_ooa':""},
        {'sta_o4a':""},
        {'sta_o3a':""},
        {'sta_ooa':""},
        {'sta_oma':""},
        {'sta_dfx':""},
        {'sta_sfx':""},
        {'ara':""},
        {'tds':""},
        {'cfx':""},
        {'ctm':""},
        {'etd':""},
        {'std':""},
        {'etm':""},
        {'est':""},
        {'a10':""},
        {'tcr':""},
        {'dfx':""},
        {'sfx':""},
        {'oma':""},
        {'ooa':""},
        {'o3a':""},
        {'o4a':""},
        {'ina':""},
        {'sus':""},
        {'man':""},
        {'sta_rwy':""}, 
        {'sta_mfx':""},
        {'cfg':""},
        {'tra':""},
        {'firstmtime':""},
        {'laststdtime':""},
        {'firststdtime':""},
        {'stdminusnowtime':-999},
        {'numstdupdates':0}
        ]
 
def build_parms(args):
    """Helper function to parse command line arguments into dictionary
        input: ArgumentParser class object
        output: dictionary of expected parameters
    """
    readDir=args.dir
    #target_date=args.target_date
    target_date=args.target_date
    outdir=args.outdir  
    parms = {"readDir":readDir,
             "target_date":target_date,
             "outdir":outdir}
    
    return(parms)
     
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = \
                 "Daily report.")
    parser.add_argument("dir", 
                        help = "Directory to obtain raw compressed TBFM SWIM.")
    parser.add_argument("target_date", 
                        help = "Local Day to Focus on.")
    parser.add_argument("--outdir", default = "./")
    parms = build_parms(parser.parse_args())
    main()