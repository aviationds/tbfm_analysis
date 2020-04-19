##@Copyright 2020. Ulrich Linder, Al Capps
##@Organization - aviationdatascience.org non-profit, through release from Penn State
##@License - This work is licensed under MIT license - https://opensource.org/licenses/MIT
## This permissive license lets you do basically whatever you want.
## Per the details of the MIT license, the software is provided "as is".
## Have fun!

## In support of Penn State Data Science, this is the first step in data parsing 
##of publicly available TBFM SWIM data
## This program parses the "air" messages in xml and places them in CSV
## format in the specified directory for later processing.
import gzip
import os
import argparse
import os.path
import time
from bs4 import BeautifulSoup as bs


###The following can be treated as static and immutable. These are for
## Writing to a zipped file directly. 
comma_enc=",".encode()
newl_enc="\n".encode()
    
## Main loop###
## searches the directory given, processes each TBFM SWIM file
## splits up the messages into air, adp, oth, con
## formats the air into CSV
def main():
 
    ldir=os.listdir(parms["readDir"])
    path=parms["readDir"]
    out_dir=parms["outdir"]

    for filename in ldir:
        print(filename)
        ## Only process files that are bzipped
        if not filename.endswith(".xml.gz"):
            continue
        full_path=path + "/" + filename

        outname=out_dir + "/" +filename + "_air.csv.gz"
        airF = gzip.open(outname, "wb")
        write_air_header(airF)

        ### Just storing raw adp messages right now
        conoutname=out_dir + "/" + filename + "_con.xml.gz"
        conF = gzip.open(conoutname, "wb")
        
        ### Just storing raw adp messages right now
        adpoutname=out_dir + "/" + filename +"_adp.xml.gz"
        adpF = gzip.open(adpoutname, "wb")
        
        ### Just storing raw oth messages right now
        othoutname=out_dir + "/" + filename +"_oth.xml.gz"
        othF = gzip.open(othoutname, "wb")

        start = time.time()
        print("Processing "+full_path + " at " +str(start),flush=True)  

        with gzip.open(full_path,'rt') as f:
         for line in f:
          str_line=str(line)
          if not "capture-timestamp" in str_line:
                ##This next step should not be necessary, but it 
                ##looks like the binary we are reading from was stored wrong
                str_line=str_line.split("b'")[1]
                
                ##airType is parsed and printed in a flat CSV format               
                if "airType=" in str_line:
                    parse_air(str_line,airF)
                elif "<con>" in str_line:
                    ##Just writing the raw data to a gzipped file
                    conF.write(str_line.encode())
                elif "<adp>" in str_line:
                    ##Just writing the raw data to a gzipped file
                    adpF.write(str_line.encode())
                elif "<oth>" in str_line:
                    ##Just writing the raw data to a gzipped file
                    othF.write(str_line.encode())
                else:
                    pass
                                   
    f.close()
    stop = time.time()
    duration = (stop - start)/60
    print("Processing took "+ str(duration) + " minutes",flush=True)
    airF.close()
    conF.close()
    adpF.close()
    othF.close()
    
### Simple method to write CSV header
def write_air_header(airF):
    
    str_header='msgtime,aid,tmaId,dap,apt,mfx,cat,gat,bcn,rwy,scn,fps,'+\
    'acs,typ,eng,spd,trw,sfz,rfz,eta_rwy,eta_mfx,eta_oma,eta_sfx,'+\
    'eta_dfx,eta_o4a,eta_o3a,eta_ooa,sta_o4a,sta_o3a,sta_ooa,sta_oma,'+\
    'sta_dfx,sta_sfx,ara,tds,cfx,ctm,etd,std,etm,est,a10,tcr,dfx,sfx,oma,'+\
    'ooa,o3a,o4a,ina,sus,man,sta_rwy,sta_mfx,cfg,tra\n'         
    airF.write(str_header.encode())     
 
##############################
###  
###  This method extracts all the useful information 
###  from the air messages and flattens them, stores them.
###  This is not efficient, but worked well at time of writing.
#############################
def parse_air(str_line,airF):
    
    ## msgd initializes all possible name value pairs with a blank "" value
    ## This come in handy later when printing all elements of each flight
    msgd={'mfx':"",'cat':"",'gat':"",'bcn':"",
          'rwy':"",'scn':"",'fps':"",'acs':"",
          'typ':"",'eng':"",'spd':"",'trw':"",
          'sfz':"",'rfz':"",'eta_rwy':"",
          'eta_mfx':"",'eta_oma':"",
          'eta_sfx':"",'eta_dfx':"",
          'eta_o4a':"",'eta_o3a':"",
          'eta_ooa':"",'sta_o4a':"",
          'sta_o3a':"",'sta_ooa':"",
          'sta_oma':"",'sta_dfx':"",
          'sta_sfx':"",'ara':"",'tds':"",
          'cfx':"",'ctm':"",'etd':"",'std':"",
          'etm':"",'est':"",'a10':"",'tcr':"",
          'dfx':"",'sfx':"",'oma':"",'ooa':"",
          'o3a':"",'o4a':"",'ina':"",'sus':"",
          'man':"",'sta_rwy':"",'sta_mfx':"",'cfg':"",
          'tra':""}            
    
    ##Here "bs" is beautiful soup, which is used for many parsing utilities          
    response = bs(str_line,"lxml")
    
    ### Here we iterate through all (or most) descendants of the XML element 
    ### and assign any populated values to the appropriate
    ### python dictionary with the same name as the element
    for child in response.descendants:
        if (child.name == "tma"):
            msgtime=child['msgtime']
            continue
        elif (child.name == "air"):
            aid=child['aid']
            apt=child['apt']
            dap=child['dap']
            tmaid=child['tmaid']           
        elif (child.name not in msgd.keys()):
            continue
        else:
            msgd.update({child.name:child.text})
            

    ### Write the elements to CSV file
    ### This section takes the longest (writing to disk)
    ##Write first few elements that are not in dictionary
    key=msgtime+","+aid+","+tmaid+","+dap+","+apt+","
    airF.write(key.encode())
    
    ##Write each populated value. Prints in order defined at beginning
    ## of this method
    for key, value in msgd.items():
        airF.write(value.encode()+comma_enc)

    ## Finish it off with a cherry on top - newline
    airF.write(newl_enc)
    

def build_parms(args):
    """Helper function to parse command line arguments into dictionary
        input: ArgumentParser class object
        output: dictionary of expected parameters
    """
    readDir=args.dir
    outdir=args.outdir  
    parms = {"readDir":readDir,
             "outdir":outdir}
    
    return(parms)
     
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = \
                 "Step1 Parsing of TBFM SWIM.")
    parser.add_argument("dir", 
                        help = "Directory to obtain raw compressed TBFM SWIM.")
    parser.add_argument("--outdir", default = "./")
    parms = build_parms(parser.parse_args())
    main()
