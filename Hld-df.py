import os
import re
import pandas as pd
import numpy as np
from pandas.core.reshape.concat import concat
import glob
import configparser
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

#### Config section for input variables from user
config = configparser.ConfigParser()
config.sections()
config.read('input_variables.ini')

####### Find Files Section ########
hld_file=glob.glob('C:\\Users\\mdhingra\\RobotFrameworkProjects\\Bharti MANO\\hld\\Testdata\\HLD*.xlsx')
cpu_file_name='Testdata/CPUUsageMeasurement.csv'
remote_file_name='Testdata/Remote_Node_Details.xlsx'
all_me_files=glob.glob('C:\\Users\\mdhingra\\RobotFrameworkProjects\\Bharti MANO\\hld\\Testdata\\ALLME_*.txt')

###### Read Files Section #########
hld_file_name=hld_file[0]
remote_file = pd.ExcelFile(remote_file_name,engine='openpyxl')
hld_file= pd.ExcelFile(hld_file_name,engine='openpyxl')
cpu_file= pd.read_csv(cpu_file_name)

###### Global dictionary and lists #######
dict_files={}
hn_dict={}
dra_lport_list1=[]
dra_lport_list2=[]
mename_list=[]

###### Variable assignment from ini file ########
HNSELECTION_DRA1=config['Default']['HNSELECTION_DRA1']
HNSELECTION_DRA2=config['Default']['HNSELECTION_DRA2']

hn_dict["HNSELECTION_DRA1"]=HNSELECTION_DRA1
hn_dict["HNSELECTION_DRA2"]=HNSELECTION_DRA2

DRA_IP_TYPE=config['Default']['DRA_IP_TYPE'] #peer names will be same {'different','same'}


###### Hardcoded dictionary assignment ########
interface_dict={'S6a': 'S6a/S6d', 
'Gy':'Gy/Ro', 
'Cx':'Cx/Dx', 
'Sh':'Sh/Dh', 
'S13':'S13/S13-Bis'
}
mated_pair_flag=False
#HOSTNAME="Not Listed" #{'Defined','Default','Not Listed'}
CPU_THRESHOLD=int(config['Default']['CPU_THRESHOLD'])
EXCLUDE_C_LINK_MODULE=config['Default']['EXCLUDE_C_LINK_MODULE']
REGPORTFLAG=config['Default']['REGPORTFLAG']

def verify_mated_pair(all_me_files,string):
    final_line=[]
    with open(all_me_files[0],'r') as file1:
        for line in file1:
            if string in line:
                final_line=line.split(',')
        for value in final_line:
            if "HN" in value:
                final_HN=value
        with open(all_me_files[1],'r') as file2:
            for line in file2:
                if "ADD DA" in line:
                    if final_HN in line:
                        mated_pair_flag=True
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
    if remote_df.shape[0]>1:
        for remote_row in range(0,remote_df[remote_df.columns[0]].count()-1):
            if (remote_df['Peer Name'][remote_row]==remote_df['Peer Name'][remote_row+1]) & (remote_df['FQDN'][remote_row]==remote_df['FQDN'][remote_row+1]) & (remote_df['Domain'][remote_row]==remote_df['Domain'][remote_row+1]):
                interface_list=remote_df['Interface'].unique().tolist()
                #if verify_linkset_group_with_interface(remote_df):
                #what if the rows have different data , assert
            else:
                logger.error("Please check Peer Name, FQDN and Domain. The rows doesn't match")
    else:
        interface_list=remote_df['Interface'].unique().tolist()
    interface_list=  [x.strip(' ') for x in interface_list]
    logger.info(interface_list)
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
        logger.info(final_interface)
        return final_interface
    else:
        logger.info(lookup_interface_list)
        logger.error("Interfaces does not match from the lookup table")

def verify_hld_interface(interface,hld_df):
    logger.info(hld_df['RemoteNode.Interface'][0]==interface)
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

def get_dra_node_daname(file,string,list):
    final_line=[]
    mename_list=[]
    with open(all_me_files[file-1],'r') as file1:  
        for line in file1:
            dict={}
            if string in line:
                final_line=line.split(',')
                for value in final_line:
                    name=value.split('=')
                    dict[name[0]]=name[1].strip("\"\"")
                mename_list.append(dict[string])
                if config['Default']['HNSELECTION_DRA'+str(file)]=="Default":
                    break
    
    if config['Default']['HNSELECTION_DRA'+str(file)]=="Not Listed":
        if config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['DANAME']==" ":
           logger.error("Please provide DANAME for DRA"+str(file))
           mename_list=[]
        else:
            if config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['DANAME'] not in mename_list:
                mename_list=[]
                mename_list.append(config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['DANAME'])
            else:
                logger.error("DANAME given should not be present in ALLME file for DRA"+str(file))
    return mename_list

def get_dra_node_mename(file,string):
    final_line=[]
    with open(file,'r') as file1:  
        for line in file1:
            dict={}
            if string in line:
                final_line=line.split(',')
                break
        for value in final_line:
            name=value.split('=')
            dict[name[0]]=name[1].strip("\"\"")
    return dict[string]


def verify_dra_name_node(hld_df,dra_name_node,i):
    return (hld_df['DRA'+str(i)+".Node"]==dra_name_node).all(axis=0)

def get_host_domain(file,string,mename,string_hn_or_dn):
    hn_dn_list=[]
    flag=True
    with open(dict_files["all_me_file"+str(file)],'r') as file1:  
        final_line=[]
        for line in file1:
            if string in line:
                final_line=line.split(',')
                dict={}
                for value in final_line:
                    name=value.split('=')
                    dict[name[0]]=name[1].strip("\";\n")
                    if (name[0]==string_hn_or_dn and dict[name[0]] not in hn_dn_list):
                        if hn_dict["HNSELECTION_DRA"+str(file)]=="Defined":
                            hn_dn_list.append(dict[name[0]])
                            daname_list.append(dict["ADD DA:DANAME"])
                            continue
                        elif  hn_dict["HNSELECTION_DRA"+str(file)]=="Default":
                            if dict["ADD DA:DANAME"]==mename:
                                hn_dn_list.append(dict[name[0]])
                                break
                if hn_dict["HNSELECTION_DRA"+str(file)]=="Not Listed":
                    if string_hn_or_dn=="HN" and (config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['DANAME'] in dict.values() and config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['HN'] in dict.values()):
                            logger.error("DRA"+str(file)+" Hostname should not have been listed in ALLME file")
                            flag=False
                    elif string_hn_or_dn=="DN" and (config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['DANAME'] in dict.values() and config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['DOMAIN'] in dict.values()):
                            logger.error("DRA"+str(file)+" Domain should not have been listed in ALLME file")
                            flag=False
                    break
    if hn_dict["HNSELECTION_DRA"+str(file)]=="Not Listed" and flag==True: 
        if string_hn_or_dn=="HN":
            hn_dn_list.append(config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['HN'])
        else:
            hn_dn_list.append(config['HN_NOT_LISTED_SECTION_DRA'+str(file)]['DOMAIN'])
    return hn_dn_list


def verify_host_domain(hld_df,list,i,string):
    if hld_df['DRA'+str(i)+"."+string][0] in list:
        return (hld_df['DRA'+str(i)+"."+string]==hld_df['DRA'+str(i)+"."+string][0]).all(axis=0)

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

def getipv4_from_allmefile(string,file):
    dict={}
    iplist=[]
    final_line=[]
    with open(dict_files["all_me_file"+str(file)],'r') as file1:
        for line in file1:
            if string in line:
                final_line=line.split(',')
            for value in final_line:
                name=value.split('=')
                dict[name[0]]=name[1].strip("\"\"")
                if name[0]=="IPV41" and dict[name[0]] not in iplist:
                    iplist.append(dict[name[0]])

def get_mid(cpu_df,file,node_name):
    mid_list=[]
    new_cpu_df=pd.to_datetime(cpu_df["result_time"])
    cpu_df=cpu_df[(cpu_df["ne_name"]==node_name)]
    cpu_df=cpu_df[new_cpu_df.dt.strftime('%H:%M:%S').between('06:00:00','23:00:00')]
    cpu_df=cpu_df[cpu_df["module"].str.contains("BSG")]
    cpu_df =cpu_df.sort_values(by = 'peak_cpu_usage', ascending = False)
    cpu_df=cpu_df.drop_duplicates(['module'])
    cpu_df = cpu_df[cpu_df['peak_cpu_usage']<CPU_THRESHOLD]
    cpu_df=cpu_df.sort_values(by = ['peak_cpu_usage','module'])
    cpu_df['new_module']=(cpu_df['module'].str.extract('(\d+)'))
    midlist= (cpu_df['new_module'].tolist())
    return midlist

def verify_mid(hld_df,file,mid_list,sheet):
    if EXCLUDE_C_LINK_MODULE=="YES":
        getipv4_from_allmefile("ADD IPADDR",file)
    else:
        if len(mid_list)==0:
            logger.error("Check for the cpu threshold value")
        elif len(mid_list)<hld_df.shape[0]:
            logger.error("Insufficient mids available for DRA"+str(file)+" in "+sheet)
        else:
            if ((hld_df["DRA"+str(file)+".MID"]).isin(mid_list[0:hld_df.shape[0]])).all(axis=0)==False:
                logger.error("DRA"+str(file)+" "+"MID doesn't match with the value inferred from CPU file for "+sheet)      
        return ((hld_df["DRA"+str(file)+".MID"]).isin(mid_list[0:hld_df.shape[0]])).all(axis=0)

def verify_primary_secondary_ip(hld_df,file,ipversion,remote_df):
    flag=True
    for i in range(0,hld_df.shape[0]):
        iplist=[]
        count=0
        final_line=[]
        dict={}
        current_mid=hld_df["DRA"+str(file)+".MID"][i]
        with open(dict_files["all_me_file"+str(file)],'r') as file1:
            for line in file1:
                if (("ADD MODULE") and "MT=BSG") in line:
                    if "MID="+str(current_mid) in line:
                        final_line=line.split(',')
                        break
            for value in final_line:
                name=value.split('=')
                dict[name[0]]=name[1]
            dict_ifm={}
            count=0   
            ifmid_list=[]
            for line in file1:
                if (("ADD MODULE") and "MT=IFM") in line:
                    if "SRN1="+dict["SRN1"] and "SN1="+dict["SN1"] in line:
                        final_line=line.split(',')
                        for value in final_line:
                            name=value.split('=')
                            dict_ifm[name[0]]=name[1]
                            if  dict_ifm["ADD MODULE:MID"] not in ifmid_list:
                                ifmid_list.append(dict_ifm["ADD MODULE:MID"])
                                count+=1
                                break
                        if remote_df["Link Homing"][0]=="Single":
                            break
                        elif count==2 and remote_df["Link Homing"][0]=="Multi":
                            break  
            logger.info(ifmid_list)
            if len(ifmid_list)==0:
                logger.error("No IFMMID found in ADD IPADDR section")
                flag=False
                break
            else:
                for ifmid in ifmid_list:
                    with open(dict_files["all_me_file"+str(file)],'r') as file1:
                        for line in file1:
                            if "ADD IPADDR:ADDRNAME" and "IPVER="+ipversion in line:
                                if "IFMMID="+str(ifmid) in line:
                                    final_line=line.split(',')
                                    break
                        for value in final_line:
                            name=value.split('=')
                            dict[name[0]]=name[1].strip("\"\"")
                            if name[0]==ipversion+"1" and dict[name[0]] not in iplist:
                                iplist.append(dict[name[0]])
            logger.info(iplist)
            if len(iplist)!=0:
                if iplist[0]==hld_df['DRA'+str(file)+'.Primary IP'][i]:
                    if remote_df["Link Homing"][0]=="Multi":
                        if iplist[1]==hld_df['DRA'+str(file)+'.Secondary IP'][i]:
                            pass
                        else:
                            logger.error("Derived Secondary ip from file"+file+" doesn't match with the value in hld file")
                    else:
                        pass
                else:
                    logger.error("Derived Primary ip from file"+file+" doesn't match with the value in hld file")
                    flag=False
    return (flag)

def verify_regport(hld_df,file,sheet):
    flag=True
    if REGPORTFLAG=="NO":
        status=(hld_df["DRA"+str(file)+".RegPortFlag"]==REGPORTFLAG).all(axis=0)
        if status:
            if hld_df["DRA"+str(file)+".Registered Port"].isnull().values.all():
                return hld_df["DRA"+str(file)+".Registered Port"].isnull().values.all()
            else:
                logger.error("Registered Port should be empty when regportflag is No for DRA"+str(file)+" in "+sheet)
                return False
        else:
            flag=False
    else:
        regport=int(config['REGPORT_SECTION']['REGPORT'])
        status=(hld_df["DRA"+str(file)+".RegPortFlag"]==REGPORTFLAG).all(axis=0)
        if status:
            with open(dict_files["all_me_file"+str(file)],'r') as file1:
                for line in file1:
                    if "REGPORTFLAG=YES" and "REGPORT="+str(regport) in line:
                        logger.error("Assigned Registered Port value should not be present in ALLME file for DRA"+str(file)+" in "+sheet)
                        return False
        else:
            flag=False
    if flag==False:
        logger.error("Registered Port Flag should be "+hld_df["DRA"+str(file)+".RegPortFlag"][0]+" for DRA"+str(file)+" in "+sheet)
        return flag
    else:
        return (hld_df["DRA"+str(file)+".Registered Port"]==regport).all(axis=0)
    

def verify_lport(hld_df,file,lport_list,sheet):
    lport=(hld_df["DRA"+str(file)+".LPort"].tolist())
    if file==1:
        if any(x in dra_lport_list1 for x in lport):
            logger.error("All LPORT should be unique for DRA1 in "+sheet)
            return False
        else:
            dra_lport_list1.append(lport)
    elif file==2:
        if any(x in dra_lport_list2 for x in lport):
            logger.error("All LPORT should be unique for DRA2 in "+sheet)
            return False
        else:
            dra_lport_list2.append(lport)
    if any(x in lport_list for x in lport):
        logger.error("Match found for DRA"+str(file)+".lport in "+sheet+". Execution should be stopped")
        return False
    else:
        return (True)

def get_lport_list(file):
    lport_list=[]
    with open(dict_files["all_me_file"+str(file)],'r') as file1:
        for line in file1:
            if "ADD DMLNK:LNKNAME" in line:
                final_line=line.split(',')
                for value in final_line:
                    name=value.split('=')
                    if name[0]=="LPORT":
                        lport_list.append(name[1])
    return lport_list

    
if __name__=='__main__':
    dra_dict={}
    if (verify_mated_pair(all_me_files,'MDA-1'))==True:
        print("Mated pair check is done")
    else:
        logger.error("This is not Mated DRA, execution should have been stopped")
    hld_df=pd.DataFrame(pd.read_excel(hld_file_name,sheet_name=hld_file.sheet_names[0],engine='openpyxl'))
    for i in range(1,len(all_me_files)+1):
        daname_list=get_dra_node_daname(i,"ADD DA:DANAME",mename_list)
        logger.info(daname_list)
        if config['Default']['HNSELECTION_DRA'+str(i)]=="Not Listed" or config['Default']['HNSELECTION_DRA'+str(i)]=="Default":
            if daname_list[0]==hld_df["DRA"+str(i)+".Node"][0]:
                dict_files["all_me_file"+str(i)]=all_me_files[i-1]
            else:
                logger.error("Mename from ALLME file doesn't match with DRA"+str(i)+" Node")
        elif config['Default']['HNSELECTION_DRA'+str(i)]=="Defined":
            if hld_df["DRA"+str(i)+".Node"][0] in daname_list:
                dict_files["all_me_file"+str(i)]=all_me_files[i-1]
            else:
                logger.error("DANAME present should be from ALLME file for DRA"+str(i))

        
    cpu_df=pd.DataFrame(cpu_file)
    size={}
    hn_col_name=["Host Name","Domain"]
    if len(dict_files)==len(all_me_files):
        for i in range(1,len(dict_files)+1):
            dra_dict["dra"+str(i)+"_node"]=get_dra_node_mename(dict_files["all_me_file"+str(i)],"MENAME")
            dra_dict["dra"+str(i)+"_hostname"]=get_host_domain(i,"ADD DA",dra_dict["dra"+str(i)+"_node"],"HN")
            dra_dict["dra"+str(i)+"_domainname"]=get_host_domain(i,"ADD DA",dra_dict["dra"+str(i)+"_node"],"DN")
            dra_dict["dra"+str(i)+"_mid"]=get_mid(cpu_df,i,dra_dict["dra"+str(i)+"_node"])
            dra_dict["dra"+str(i)+"_lport"]=get_lport_list(i)
        col_names=["Peer Name","FQDN","Domain","Protocol","IP version","Local Port","Node Role"]
        for remote_sheet,hld_sheet in zip(remote_file.sheet_names,hld_file.sheet_names):
            remote_df=pd.DataFrame(pd.read_excel(remote_file_name,sheet_name=remote_sheet,engine='openpyxl'))
            hld_df=pd.DataFrame(pd.read_excel(hld_file_name,sheet_name=hld_sheet,engine='openpyxl'))
            size[remote_sheet]=hld_df.shape[0]
            verify_site=(verify_remote_hld_column(remote_df,hld_df,"Site Name"))
            if DRA_IP_TYPE=="Different":
                if verify_site==True:
                    print("Site Name for all rows matches in input and output for "+remote_sheet)
                else:
                    logger.error("Site Name doesn't match for "+remote_sheet)
                #verify_remote_interface(remote_df)
                interface=get_interface(remote_df,hld_df)
                logger.info(interface)
                if verify_hld_interface(interface,hld_df)==True:
                    print("Interfaces inferred from the input file and generated as per the output matches with the output interfaces for "+remote_sheet)
                else:
                    logger.error("Interface inferred doesn't match for "+remote_sheet)

                for col_name in col_names:
                    if verify_remote_hld_column(remote_df,hld_df,col_name):
                        print(col_name+" for all rows matches in input and output for "+remote_sheet)
                    else:
                        logger.error(col_name+" doesn't match for "+remote_sheet)
                
                if verify_link_homing(remote_df):
                    if verify_remote_hld_column(remote_df,hld_df,"Primary IP"):
                        print("Primary IP values for all rows matches in input and output for "+remote_sheet)
                    else:
                        logger.error("Primary IP doesn't match for "+remote_sheet)
                    if (remote_df['Link Homing']=="Multi").all(axis=0):
                        if verify_remote_hld_column(remote_df,hld_df,"Secondary IP"):
                            print("Secondary IP values for all rows matches in input and output for "+remote_sheet)
                        else:
                            logger.error("Secondary IP doesn't match for "+remote_sheet)

                for i in range(1,len(dict_files)+1):
                    if verify_dra_name_node(hld_df,hld_df["DRA"+str(i)+".Node"][0],i):
                        print("DRA"+str(i)+" NAME NODE for all rows matches with the value found in the "+hld_sheet)
                    else:
                        logger.error("DRA"+str(i)+" NAME NODE doesn't match with the value found in the "+hld_sheet)
                    list_name=[]
                    for name in hn_col_name:
                        if name=="Host Name":
                            list_name=dra_dict["dra"+str(i)+"_hostname"]
                        else:
                            list_name=dra_dict["dra"+str(i)+"_domainname"]
                        if verify_host_domain(hld_df,list_name,i,name):
                            print("DRA"+str(i)+" "+str(name)+" for all rows matches with the value found in the "+hld_sheet)
                        else:
                            logger.error("DRA"+str(i)+" "+str(name)+" doesn't match with the value found in the "+hld_sheet)
                        
                    
                    if (verify_linkset_name(remote_df,hld_df,i,remote_sheet)):
                        print("DRA"+str(i)+" LinkSet Name for all rows matches with the value inferred for "+hld_sheet)
                        print("DRA"+str(i)+" Link Name for all rows matches with the value inferred for "+hld_sheet)
                    
                    if len(dra_dict["dra"+str(i)+"_mid"])!=0:
                        if verify_mid(hld_df,i,dra_dict["dra"+str(i)+"_mid"],hld_sheet):
                            print("DRA"+str(i)+" MID for all rows matches with the value inferred from CPU file for "+hld_sheet)
                
                    if verify_primary_secondary_ip(hld_df,i,hld_df["RemoteNode.IPv4/IPv6"][0],remote_df):
                        print("DRA"+str(i)+" Primary/Secondary IP for all rows matches with the value inferred from ALLME file for "+hld_sheet)
                    else:
                        logger.error("DRA"+str(i)+" "+"Primary/Secondary IP  doesn't match with the value inferred from ALLME file for "+hld_sheet)

                    if verify_regport(hld_df,i,hld_sheet):
                        print("DRA"+str(i)+" RegPortFlag and Registered Port column verified for "+hld_sheet)

                    if verify_lport(hld_df,i,dra_dict["dra"+str(i)+"_lport"],hld_sheet):
                        print("DRA"+str(i)+" LPORT column has been verified for "+hld_sheet)
                print("*****************************************"+hld_sheet+" columns verified ******************************************")


if os.stat("newfile.log").st_size!=0:
    print("***************Check the log file for errors*******************")
