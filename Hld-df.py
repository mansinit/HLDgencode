import re
import pandas as pd
import numpy as np
from pandas.core.reshape.concat import concat
remote_file_name='TC-1 Final/2150/Remote_Node_Details.xlsx'
remote_file = pd.ExcelFile(remote_file_name,engine='openpyxl')
hld_file_name='TC-1 Final/2150/HLD_NEWNODE_2150.xlsx'
hld_file= pd.ExcelFile(hld_file_name,engine='openpyxl')
all_me_file1="C://Users//mdhingra//RobotFrameworkProjects//Bharti MANO//hld//TC-1 Final//2150//ALLME_20210614033028.txt"
all_me_file2='C://Users//mdhingra//RobotFrameworkProjects//Bharti MANO//hld//TC-1 Final//2150//ALLME_20210614033049.txt'
mated_pair_flag=False
DRA_IP_TYPE="Different" #peer names will be same {'different','same'}
interface_dict={'S6a': 'S6a/S6d', 
'Gy':'Gy/Ro', 
'Cx':'Cx/Dx', 
'Sh':'Sh/Dh', 
'S13':'S13/S13-Bis'
}
HOSTNAME="Default" #{'Defined','Default','Not Listed'}
# importing module
import logging
# Create and configure logger
logging.basicConfig(filename="newfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
# Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

def verify_mated_pair(all_me_file1,all_me_file2,string):
    final_line=[]
    with open(all_me_file1,'r') as file1:
        for line in file1:
            if string in line:
                final_line=line.split(',')
        print(final_line)
        for value in final_line:
            if "HN" in value:
                final_HN=value
        with open(all_me_file2,'r') as file2:
            for line in file2:
                if "ADD DA" in line:
                    if final_HN in line:
                        mated_pair_flag=True
                        print("Mated Pair Verified")
                        break
    return mated_pair_flag

def verify_remote_hld_column(remote_df,hld_df,col_name):
    #this will compare all the rows of Site Name column in remote and hld file.
    #all should have the same number of rows and same value, it will return True
    if col_name=="Site Name":
        return ((remote_df['Site Name'] == hld_df['RemoteNode.SiteName']).all(axis=0))
    if col_name=="Peer Name":
        return ((remote_df['Peer Name'] == hld_df['RemoteNode.Peername']).all(axis=0))
    if col_name=="FQDN" or col_name=="Domain" or col_name=="Protocol" or col_name=="Primary IP" or col_name=="Secondary IP":
        return ((remote_df[col_name] == hld_df['RemoteNode.'+col_name]).all(axis=0))
    if col_name=="IP version":
        return ((remote_df['IP version'] == hld_df['RemoteNode.IPv4/IPv6']).all(axis=0))
    if col_name=="Local Port":
        return ((remote_df['Local Port'] == hld_df['RemoteNode.LPort']).all(axis=0))
    if col_name=="Node Role":
        return ((remote_df['Node Role'] == hld_df['RemoteNode.NodeRole']).all(axis=0))

def verify_linkset_group_with_interface(remote_df):
    
    for remote_row in range(0,remote_df[remote_df.columns[0]].count()-1):
        count=0
        i=remote_row
        while remote_df['LinkSet Group'][remote_row]==remote_df['LinkSet Group'][remote_row+1]:
            count += 1
        while i<count:
            if remote_df['Interface'][i]==remote_df['Interface'][i+1]:
                pass
            else:
                raise(Exception)
        else:
            raise(Exception)

def verify_remote_interface(remote_df,remote_interface_list):
    interface_list=["S6a","Cx", "Sh" ,"Gx" ,"Rx" ,"Gy" ,"S6b" ,"SWx" ,"SWm" ,"S13" ,"SLg" ,"SLh"]
    return all(item in interface_list for item in remote_interface_list)

def get_interface(remote_df,hld_df):
    interface_list=[]
    deconc_interface_list=[]
    print(remote_df[remote_df.columns[0]].count())
    for remote_row in range(0,remote_df[remote_df.columns[0]].count()-1):
        if (remote_df['Peer Name'][remote_row]==remote_df['Peer Name'][remote_row+1]) & (remote_df['FQDN'][remote_row]==remote_df['FQDN'][remote_row+1]) & (remote_df['Domain'][remote_row]==remote_df['Domain'][remote_row+1]):
            interface_list=remote_df['Interface'].unique().tolist()
            #if verify_linkset_group_with_interface(remote_df):
            #what if the rows have different data , assert
        else:
            logger.error("Please check Peer Name, FQDN and Domain. The rows doesn't match")
    for interface in interface_list:
        if ',' not in interface:
            deconc_interface_list.append(interface)
        else:
            for deconc_interface in interface.split(','):
                if deconc_interface not in deconc_interface_list:
                    deconc_interface_list.append(deconc_interface)
                #if interface.split(',') not in list 
    lookup_interface_list= [(interface_dict[interface])+"-1" if interface in interface_dict.keys() else (interface)+"-1" for interface in deconc_interface_list]
    if (verify_remote_interface(remote_df,deconc_interface_list))==True:
        print("All the interfaces are verified for: "+remote_sheet)
        final_interface='&'.join(lookup_interface_list)
        return final_interface
    else:
        logger.error("Interfaces does not match from the lookup table")

def verify_hld_interface(interface,hld_df):
    return ((hld_df['RemoteNode.Interface'] == interface).all(axis=0))

def verify_link_homing(remote_df):
    flag_for_multi_or_single_ip=False
    row_count=remote_df[remote_df.columns[0]].count()
    if (remote_df['Link Homing']=="Multi").all(axis=0):
        if (remote_df['Secondary IP'].isnull().sum()==row_count) or (remote_df['Primary IP'].isnull().sum()==row_count):
            logger.error("Multi Link Homing should have both Primary IP and Secondary IP")
        elif (remote_df['Secondary IP'].isnull().sum()>0) or (remote_df['Primary IP'].isnull().sum()>0):
            logger.error("One or more missing values for Primary or Secondary IP")
        else:
            flag_for_multi_or_single_ip=True
    elif (remote_df['Link Homing']=="Single").all(axis=0):
        if remote_df['Secondary IP'].isnull().values.all() and not(remote_df['Primary IP'].isnull().values.any()):
            flag_for_multi_or_single_ip=True
        elif not( remote_df['Secondary IP'].isnull().values.all()):
            logger.error("Single Link Homing should have only Primary IP")
        elif remote_df['Primary IP'].isnull().values.any():
            logger.error("One or more missing values for Primary IP")
    return flag_for_multi_or_single_ip

def get_dra_node_mename(file,string):
    final_line=[]
    with open(file,'r') as file1:  
        for line in file1:
            if string in line:
                final_line=line.split(',')
        dict={}
        for value in final_line:
            name=value.split('=')
            dict[name[0]]=name[1].strip("\"\"")
    return dict[string]

def verify_dra_name_node(hld_df,dra_name_node,i):
    return (hld_df['DRA'+str(i)+".Node"]==dra_name_node).all(axis=0)

def get_hostname(file,string,mename):
    hostname_list=[]
    with open(file,'r') as file1:  
        final_line=[]
        for line in file1:
            if string in line:
                final_line=line.split(',')
                dict={}
                for value in final_line:
                    name=value.split('=')
                    dict[name[0]]=name[1].strip("\";\n")
                    if name[0]=="HN" and dict[name[0]] not in hostname_list:
                        if HOSTNAME=="Defined" or HOSTNAME=="Not Listed":
                            hostname_list.append(dict[name[0]])
                            continue
                        elif HOSTNAME=="Default":
                            if dict["ADD DA:DANAME"]==mename:
                                hostname_list.append(dict[name[0]])
                                break
    return hostname_list


def verify_hostname(hld_df,hostname_list,i):
    print(hld_df['DRA'+str(i)+".Host Name"][0])
    if hld_df['DRA'+str(i)+".Host Name"][0] in hostname_list:
        return (hld_df['DRA'+str(i)+".Host Name"]==hld_df['DRA'+str(i)+".Host Name"][0]).all(axis=0)

def verify_linkset_name(remote_df,hld_df,file,remote_sheet):
    #for remote_row in range(0,(remote_df[remote_df.columns[0]].count())-1):
    #print(remote_df["Number of Links"][remote_row])
        i=0
        flag=True
        new_remote_df=remote_df.loc[remote_df["Number of Links"].notnull()]
        for new_remote_row in range(0,(new_remote_df.shape[0])):
            no_of_links=int(new_remote_df["Number of Links"][i])
            new_link=i+no_of_links
            for row in range(i,new_link):      
                if ( remote_df['LinkSet Name'].isnull().values.all()):
                    conc_interface=re.sub(",","_",remote_df["Interface"][row]) if "," in remote_df["Interface"][row] else remote_df["Interface"][row]
                    linkset_name=hld_df["DRA"+str(file)+".Node"][row]+"_"+remote_df["Peer Name"][row]+"_"+conc_interface
                   
                else:
                    linkset_name=remote_df['LinkSet Name'][row]
                if i!=0:
                    link_name=linkset_name+"_"+f"{row-i:02}"
                else:
                    link_name=linkset_name+"_"+f"{row:02}"
                if hld_df["DRA"+str(file)+".LinkSet Name"][row]==linkset_name :
                    pass
                else:
                    logger.error("Link Set Name is not correct for DRA"+str(file)+" in "+remote_sheet)
                    flag=False
                if hld_df["DRA"+str(file)+".Link Name"][row]==link_name:
                    pass
                else:
                    logger.error("Link Name is not correct for DRA"+str(file)+" in "+remote_sheet)
                    flag=False
                
            i=new_link
        return flag 

if (verify_mated_pair(all_me_file1,all_me_file2,'MDA-1'))==True:
    dra_dict={}
    files=[all_me_file1,all_me_file2]
    for i in range(1,len(files)+1):
        dra_dict["dra"+str(i)+"_node"]=get_dra_node_mename(files[i-1],"MENAME")
        dra_dict["dra"+str(i)+"_hostname"]=get_hostname(files[i-1],"ADD DA",dra_dict["dra"+str(i)+"_node"])
    print(dra_dict)
    col_names=["Peer Name","FQDN","Domain","Protocol","IP version","Local Port","Node Role"]
    for remote_sheet,hld_sheet in zip(remote_file.sheet_names,hld_file.sheet_names):
        remote_df=pd.DataFrame(pd.read_excel(remote_file_name,sheet_name=remote_sheet,engine='openpyxl'))
        hld_df=pd.DataFrame(pd.read_excel(hld_file_name,sheet_name=hld_sheet,engine='openpyxl'))
        #print(remote_df["Peer Name"][2])
        #print(hld_df["RemoteNode.SiteName"][2])
        verify_site=(verify_remote_hld_column(remote_df,hld_df,"Site Name"))
        if DRA_IP_TYPE=="Different":
            if verify_site==True:
                print("Site Name for all rows matches in input and output for "+remote_sheet)
            else:
                print("***************FAILED*******************")
                logger.error("Site Name doesn't match for "+remote_sheet)
            #verify_remote_interface(remote_df)
            interface=get_interface(remote_df,hld_df)
            print(interface)

            if verify_hld_interface(interface,hld_df)==True:
                print("Interfaces inferred from the input file and generated as per the output matches with the output interfaces for "+remote_sheet)
            else:
                print("***************FAILED*******************")
                logger.error("Interface inferred doesn't match for "+remote_sheet)

            for col_name in col_names:
                if verify_remote_hld_column(remote_df,hld_df,col_name):
                    print(col_name+" for all rows matches in input and output for "+remote_sheet)
                else:
                    print("***************FAILED*******************")
                    logger.error(col_name+" doesn't match for "+remote_sheet)
            
            if verify_link_homing(remote_df):
                if verify_remote_hld_column(remote_df,hld_df,"Primary IP"):
                    print("Primary IP values for all rows matches in input and output for "+remote_sheet)
                else:
                    print("***************FAILED*******************")
                    logger.error("Primary IP doesn't match for "+remote_sheet)
                if (remote_df['Link Homing']=="Multi").all(axis=0):
                    if verify_remote_hld_column(remote_df,hld_df,"Secondary IP"):
                        print("Secondary IP values for all rows matches in input and output for "+remote_sheet)
                    else:
                        print("***************FAILED*******************")
                        logger.error("Secondary IP doesn't match for "+remote_sheet)
           
            for i in range(1,len(files)+1):
                if verify_dra_name_node(hld_df,dra_dict["dra"+str(i)+"_node"],i):
                    print("DRA"+str(i)+" NAME NODE for all rows matches with the value found in the "+hld_sheet)
                else:
                    print("***************FAILED*******************")
                    logger.error("DRA"+str(i)+" NAME NODE doesn't match with the value found in the "+hld_sheet)
                if verify_hostname(hld_df,dra_dict["dra"+str(i)+"_hostname"],i):
                    print("DRA"+str(i)+" HOST NAME for all rows matches with the value found in the "+hld_sheet)
                else:
                    print("***************FAILED*******************")
                    logger.error("DRA"+str(i)+" HOST NAME doesn't match with the value found in the "+hld_sheet)
                
                
                if (verify_linkset_name(remote_df,hld_df,i,remote_sheet)):
                    print("DRA"+str(i)+" LinkSet Name for all rows matches with the value inferred")
                    print("DRA"+str(i)+" Link Name for all rows matches with the value inferred")
                else:
                    print("***************FAILED*******************")
            
                
            
else:
    print("***************FAILED*******************")
    logger.error("This is not Mated DRA")
