from flask import Flask, render_template, request
import pyhdb
import os
import pandas as pd
import webbrowser
from threading import Timer
import time
import statistics
import datetime
import numpy as np
from selenium import webdriver
import time
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

        
app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))
@app.route('/')
def index():
    return render_template('front.html')

@app.route('/individual', methods = ['GET','POST'])
def individual():
    return render_template('templates.html')

@app.route('/create-service', methods = ['GET','POST'])
def service():
    if request.method=='POST':
        if request.form['submit_button']=='Perform RPA':
            cap = DesiredCapabilities().INTERNETEXPLORER
            cap['ignoreProtectedModeSettings'] = True
            cap['IntroduceInstabilityByIgnoringProtectedModeSettings'] = True
            cap['nativeEvents'] = True
            cap['ignoreZoomSetting'] = True
            cap['requireWindowFocus'] = True
            cap['INTRODUCE_FLAKINESS_BY_IGNORING_SECURITY_DOMAINS'] = True
            driver = webdriver.Ie(capabilities=cap,executable_path=r'C:/Users/jry158538/Desktop/IEDriver/IEDriverServer.exe')
            driver.get("https://amat.service-now.com/help?id=sc_cat_item3&sys_id=4a57a492dbe11f40c6865f30cf961935&table=sc_cat_item_producer")
            
            def multiselect_set_selections(driver, element_id, labels):
                el = driver.find_element_by_id(element_id)
                for option in el.find_elements_by_tag_name('option'):
                    if option.text in labels:
                        option.click()
            
            #front page
            
            #driver.find_element_by_xpath("//select[@name='preferred_contact_method']/option[text()='Email']").click()
            #testing = driver.switch_to_frame(driver.find_element_by_id("sys_original.IO:b1e5ba4edbc3abc05f199b3c8a961936"))
            #testing.send_keys("Testing")
            #drop = WebDriverWait(driver, 5).until(ec.presence_of_element_located((By.ID, 's2id_sp_formfield_{{::field.name}}'))).click()
            #driver.find_element_by_xpath("//select/option[@value='3']").click()
            # click the dropdown button
            #change_summary = driver.find_element_by_id("s2id_autogen1")
            #select = Select(driver.find_element_by_id('sp_formfield_urgency'))
            #driver.find_element_by_xpath("//select[@id='sp_formfield_urgency']/option[@value='3-Medium']").click()
            # find all list elements in the dropdown. 
            # target the parent of the button for the list
            time.sleep(15)
            driver.quit()
            return render_template('templates.html')

@app.route('/get-jobs', methods = ['GET','POST'])
def my_link():
    bar = request.form['variable']

    connection = pyhdb.connect(host = "cfgavsapp", port = 39015, user = "SYSTEM", password = "Om@ha13!")
    
    cursor = connection.cursor()
    cursor.execute(("select jobname, sum(case when status ='F' then 1 else 0 end) as Success, sum(case when status ='A' then 1 else 0 end) as Fail  from P08_CAS_MLDATA.TBTCO where jobname ='{b}' group by jobname" ).format(b = bar))
    a = cursor.fetchall()
    
    cursora1 = connection.cursor()
    cursora1.execute(("select endtime,strttime, reldate from P08_CAS_MLDATA.TBTCO where jobname = '{e}'" ).format(e=bar))
    a1 = cursora1.fetchall()
    
    cursor1 = connection.cursor()
    cursor1.execute(("select jobname, sum(case when authcknam like '%SAPOP%' then 1 else 0 end) as SAPOP_EXIST,sum(case when authcknam not like '%SAPOP%' then 1 else 0 end) as SAPOP_NOT,sum(case when sdluname like '%FF%' then 1 else 0 end) as FF_EXIST,sum(case when sdluname not like '%FF%' then 1 else 0 end) as FF_NOT  from P08_CAS_MLDATA.TBTCP where jobname = '{c}' group by jobname" ).format(c = bar))
    b = cursor1.fetchall()
    
    cursor2 = connection.cursor()
    cursor2.execute(("select * from P08_CAS_MLDATA.BCS_DATA where jobname = '{d}'" ).format(d=bar))
    c = cursor2.fetchall()
    
    bcs_num = []
    systems = []
    statuses = []
    completion_rate = []
    valid_rate = []
    sap_account = []
    runtimes =[]
    x_runtimes=[]
    x30_runtimes = []
    final_average=[]
    
    list1 = []
    list2 = []
    list3 = []
    x_list1 =[]
    x_list2=[]
    x_list3 = []

    x30_list1 =[]
    x30_list2=[]
    x30_list3 = []
    
    FF_ID = []
    
    final_pass = []

    
    if len(a) >0:
        df1= pd.DataFrame(a)
        
        df1.rename(columns={'0':'Name', '1':'Success', '2':'Fail'})
        df1.columns=['Name', 'Success', 'Fail']
        df1['Runs'] = df1['Success'] + df1['Fail']
        
        df1['Success_rate'] = df1['Success']/df1['Runs']
        
        
        for success, name2 in zip(df1['Success_rate'], df1['Name']):
            if bar==name2:
                completion_rate.append(success)
                if success >0.8:
                    valid_rate.append('Yes')
                elif success <0.8:
                    valid_rate.append('No')
                    
    else:
        completion_rate.append(0)
        valid_rate.append('N/A')
    if len(a1)>0:
        dfa1= pd.DataFrame(a1)
        dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
        dfa1.columns=['Endtime','Startime', 'Date']
        
        dfa1['Endtime'] = dfa1['Endtime'].dropna()
        dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Endtime'].dropna(inplace= True)
        
        dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
        dfa1['Endtime'].dropna(inplace = True)
        
        dfa1['Startime'] = dfa1['Startime'].dropna()
        #df2['Endtime'] = df2['Endtime'].astype('int64')
        dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Startime'].dropna(inplace= True)
        
        dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
        dfa1['Startime'].dropna(inplace = True)
        dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
        
        means = dfa1['avg'].mean()

        mins = means/60
        hours = means/3600
        if means>60 and means <3600:
            list1.append(mins)
        else:
            list1.append(0)
        if means < 60:
            list2.append(means)
        else:
            list2.append(0)
        if means>3600:
            list3.append(hours)
        else:
            list3.append(0)
        
        for num, num1, num2 in zip(list3,list1,list2):
            runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))

        def x():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("10day")]
            
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x_list1.append(mins)
            else:
                x_list1.append(0)
            if means < 60:
                x_list2.append(means)
            else:
                x_list2.append(0)
            if means>3600:
                x_list3.append(hours)
            else:
                x_list3.append(0)
            
            for num, num1, num2 in zip(x_list3,x_list1,x_list2):
                x_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        
        x()
        def x30():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("30day")]
        
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x30_list1.append(mins)
            else:
                x30_list1.append(0)
            if means < 60:
                x30_list2.append(means)
            else:
                x30_list2.append(0)
            if means>3600:
                x30_list3.append(hours)
            else:
                x30_list3.append(0)
            
            for num, num1, num2 in zip(x30_list3,x30_list1,x30_list2):
                x30_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        x30()
        
        for num, num1, num2, num3, num4, num5 in zip(x_list1, x_list2,x_list3, x30_list1, x30_list2,x30_list3):
            total10 = num+num1+num2
            total30 = num3+num4+num5
            if total10>0 and total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            elif total10>0 or total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            else:
                final_average.append(0)

    else:
        final_average.append(0)
        runtimes.append(0)
        x_runtimes.append(0)
        x30_runtimes.append(0)
    if len(b)>0:
        
        df2= pd.DataFrame(b)
        
        df2.rename(columns={'0':'Name', '1':'SAP_EXIST','2':'SAP_NOT', '3':'FF_EXIST','4':'FF_NOT'})
        df2.columns=['Name', 'SAP_EXIST','SAP_NOT', 'FF_EXIST','FF_NOT']
        
        df2['SAP_TOTAL'] = df2['SAP_EXIST']+df2['SAP_NOT']
        
        df2['FF_TOTAL'] = df2['FF_EXIST'] + df2['FF_NOT']
        
        df2['SAPOP'] = df2['SAP_EXIST']/df2['SAP_TOTAL']
        
        df2['FFOP'] = df2['FF_EXIST']/df2['FF_TOTAL']
        
        for name3, sap1 in zip(df2['Name'], df2['SAPOP']):
            if bar == name3:
                if sap1 == 1.0:
                    sap_account.append('Compliant')
                else:
                    sap_account.append('Non-compliant')
        
        for name3, f_id in zip(df2['Name'], df2['FFOP']):
            if bar == name3:
                if f_id == 1.0:
                    FF_ID.append('Compliant')
                else:
                    FF_ID.append('Non-compliant')
                
    else:
        FF_ID.append('Non-compliant')
        sap_account.append('Non-compliant')
    
    
    if len(c)>0:
        
        df3= pd.DataFrame(c)
    
        df3.columns = ['Job Status', 'Title', 'JOBNUM', 'ows_Projects', 'ows_FunctionalGroup', 'ows_FunctionalArea', 'ows_Application', 'RANGE', 'JOBNAME', 'SAP_SYSTEM', 'ows_Created', 'ows_Job Criticality', 'ows_Job Summary', 'ows_Job Description', 'ows_Job Purpose', 'FREQUENCY', 'ows_Author', 'ows_Functional Contact', 'ows_Modified', 'ows_Editor', 'JOB_OWNER', 'ows_Job Type', 'ows_Prd_Aprd_By', 'ows_Quality_arv_by', 'ows_Restart Procedure', 'ows_Start Time', 'ows_Technical Contact', 'ows_Threshold_Ticketing_Enabled', 'ows_Time Zone', 'ows_Title', 'ows_Trigger Type', 'ows__UIVersionString', 'ows_Support Team Email', 'ows_Support Team', 'ows_Comments', 'ows_Nxt_Rec_Date', 'STEP', 'Varian_1', 'Program_1', 'ows_QADateHist', 'ows_Quality_aprd_On', 'ows_Certified Date', 'ows_Certified By', 'ows_Schedular Approved O', 'ows_StepNumber_2', 'ows_Varian_2', 'ows_Program_2', 'ows_StepNumber_10', 'ows_StepNumber_3', 'ows_StepNumber_4', 'ows_StepNumber_5', 'ows_StepNumber_6', 'ows_StepNumber_7', 'ows_StepNumber_8', 'ows_StepNumber_9', 'ows_Varian_10', 'ows_Varian_3', 'ows_Varian_4', 'ows_Varian_5', 'ows_Varian_6', 'ows_Varian_7', 'ows_Varian_8', 'ows_Varian_9', 'ows_StepNumber_11', 'ows_Varian_11', 'ows_Tigger Name', 'ows_StepNumber_12', 'ows_StepNumber_13', 'ows_StepNumber_14', 'ows_StepNumber_15', 'ows_StepNumber_16', 'ows_Varian_12', 'ows_Varian_13', 'ows_Varian_14', 'ows_Varian_15', 'ows_Varian_16', 'ows_EmailTes']
    
        for system, name, status, number in zip(df3['SAP_SYSTEM'], df3['JOBNAME'], df3['Job Status'], df3['JOBNUM']):
            if bar==name:
                systems.append(system)
                bcs_num.append(number)
                if status == 'Active':
                    statuses.append('100%')
                else:
                    statuses.append('0%')
                    
    else:
        bcs_num.append('N/A')
        systems.append('N/A')
        statuses.append('N/A')
        
    for c1, c2, c3, p2, p4 in zip(statuses, FF_ID,sap_account,valid_rate,final_average ):
        new_c1 = []
        new_c2 = []
        new_c3 = []
        new_p2 = []
        new_p4 = []
        if c1 == "100%":
            new_c1.append(20)
        else:
            new_c1.append(0)
        if c2 == "Compliant":
            new_c2.append(20)
        else:
            new_c2.append(0)
        if c3 == "Compliant":
            new_c3.append(20)
        else:
            new_c3.append(0)
        if p2 == "Yes":
            new_p2.append(20)
        else:
            new_p2.append(0)
        if p4 == 0:
            new_p4.append(0)
        elif "Runtime improvement efficiency is faster by:" in p4:
            new_p4.append(20)
        else:
            new_p4.append(0)
        
        for nc1, nc2,nc3,np2,np4 in zip(new_c1, new_c2, new_c3, new_p2, new_p4):
            pass_num = (nc1+nc2+nc3+np2+np4)/100
            final_pass.append(pass_num)
            
        
 
    for pass_nums,FF,final_system, x_run,b_num,sap,final_status, final_completion, final_valid, avg_runtime in zip(final_pass,FF_ID,systems,final_average,bcs_num,sap_account, statuses, completion_rate, valid_rate, runtimes):
            return render_template('templates.html',pass_id='{:.1%}'.format(pass_nums),FFID = '{}'.format(FF),color = 'red',x_runtime='{}'.format(x_run),BID='{}'.format(b_num),SID = '{}'.format(final_system), Jname='{}'.format(bar), Match='100%', SAPOP = '{}'.format(sap), Status = '{}'.format(final_status), Rate = '{:.1%}'.format(final_completion), Valid_rate ='{}'.format(final_valid), runtime = '{}'.format(avg_runtime))

@app.route('/get-jobs/<key>', methods = ['GET','POST'])
def get_jobs(key):
    bar = key

    connection = pyhdb.connect(host = "cfgavsapp", port = 39015, user = "SYSTEM", password = "Om@ha13!")
    
    cursor = connection.cursor()
    cursor.execute(("select jobname, sum(case when status ='F' then 1 else 0 end) as Success, sum(case when status ='A' then 1 else 0 end) as Fail  from P08_CAS_MLDATA.TBTCO where jobname ='{b}' group by jobname" ).format(b = bar))
    a = cursor.fetchall()
    
    cursora1 = connection.cursor()
    cursora1.execute(("select endtime,strttime, reldate from P08_CAS_MLDATA.TBTCO where jobname = '{e}'" ).format(e=bar))
    a1 = cursora1.fetchall()
    
    cursor1 = connection.cursor()
    cursor1.execute(("select jobname, sum(case when authcknam like '%SAPOP%' then 1 else 0 end) as SAPOP_EXIST,sum(case when authcknam not like '%SAPOP%' then 1 else 0 end) as SAPOP_NOT,sum(case when sdluname like '%FF%' then 1 else 0 end) as FF_EXIST,sum(case when sdluname not like '%FF%' then 1 else 0 end) as FF_NOT  from P08_CAS_MLDATA.TBTCP where jobname = '{c}' group by jobname" ).format(c = bar))
    b = cursor1.fetchall()
    
    cursor2 = connection.cursor()
    cursor2.execute(("select * from P08_CAS_MLDATA.BCS_DATA where jobname = '{d}'" ).format(d=bar))
    c = cursor2.fetchall()
    
    bcs_num = []
    systems = []
    statuses = []
    completion_rate = []
    valid_rate = []
    sap_account = []
    runtimes =[]
    x_runtimes=[]
    x30_runtimes = []
    final_average=[]
    
    list1 = []
    list2 = []
    list3 = []
    x_list1 =[]
    x_list2=[]
    x_list3 = []

    x30_list1 =[]
    x30_list2=[]
    x30_list3 = []
    
    FF_ID = []
    
    final_pass = []

    
    if len(a) >0:
        df1= pd.DataFrame(a)
        
        df1.rename(columns={'0':'Name', '1':'Success', '2':'Fail'})
        df1.columns=['Name', 'Success', 'Fail']
        df1['Runs'] = df1['Success'] + df1['Fail']
        
        df1['Success_rate'] = df1['Success']/df1['Runs']
        
        
        for success, name2 in zip(df1['Success_rate'], df1['Name']):
            if bar==name2:
                completion_rate.append(success)
                if success >0.8:
                    valid_rate.append('Yes')
                elif success <0.8:
                    valid_rate.append('No')
                    
    else:
        completion_rate.append(0)
        valid_rate.append('N/A')
    if len(a1)>0:
        dfa1= pd.DataFrame(a1)
        dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
        dfa1.columns=['Endtime','Startime', 'Date']
        
        dfa1['Endtime'] = dfa1['Endtime'].dropna()
        dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Endtime'].dropna(inplace= True)
        
        dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
        dfa1['Endtime'].dropna(inplace = True)
        
        dfa1['Startime'] = dfa1['Startime'].dropna()
        #df2['Endtime'] = df2['Endtime'].astype('int64')
        dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Startime'].dropna(inplace= True)
        
        dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
        dfa1['Startime'].dropna(inplace = True)
        dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
        
        means = dfa1['avg'].mean()

        mins = means/60
        hours = means/3600
        if means>60 and means <3600:
            list1.append(mins)
        else:
            list1.append(0)
        if means < 60:
            list2.append(means)
        else:
            list2.append(0)
        if means>3600:
            list3.append(hours)
        else:
            list3.append(0)
        
        for num, num1, num2 in zip(list3,list1,list2):
            runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))

        def x():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("10day")]
            
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x_list1.append(mins)
            else:
                x_list1.append(0)
            if means < 60:
                x_list2.append(means)
            else:
                x_list2.append(0)
            if means>3600:
                x_list3.append(hours)
            else:
                x_list3.append(0)
            
            for num, num1, num2 in zip(x_list3,x_list1,x_list2):
                x_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        
        x()
        def x30():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("30day")]
        
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x30_list1.append(mins)
            else:
                x30_list1.append(0)
            if means < 60:
                x30_list2.append(means)
            else:
                x30_list2.append(0)
            if means>3600:
                x30_list3.append(hours)
            else:
                x30_list3.append(0)
            
            for num, num1, num2 in zip(x30_list3,x30_list1,x30_list2):
                x30_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        x30()

        for num, num1, num2, num3, num4, num5 in zip(x_list1, x_list2,x_list3, x30_list1, x30_list2,x30_list3):
            total10 = num+num1+num2
            total30 = num3+num4+num5
            if total10>0 and total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            elif total10>0 or total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            else:
                final_average.append(0)

    else:
        final_average.append(0)
        runtimes.append(0)
        x_runtimes.append(0)
        x30_runtimes.append(0)
    if len(b)>0:
        
        df2= pd.DataFrame(b)
        
        df2.rename(columns={'0':'Name', '1':'SAP_EXIST','2':'SAP_NOT', '3':'FF_EXIST','4':'FF_NOT'})
        df2.columns=['Name', 'SAP_EXIST','SAP_NOT', 'FF_EXIST','FF_NOT']
        
        df2['SAP_TOTAL'] = df2['SAP_EXIST']+df2['SAP_NOT']
        
        df2['FF_TOTAL'] = df2['FF_EXIST'] + df2['FF_NOT']
        
        df2['SAPOP'] = df2['SAP_EXIST']/df2['SAP_TOTAL']
        
        df2['FFOP'] = df2['FF_EXIST']/df2['FF_TOTAL']
        
        for name3, sap1 in zip(df2['Name'], df2['SAPOP']):
            if bar == name3:
                if sap1 == 1.0:
                    sap_account.append('Compliant')
                else:
                    sap_account.append('Non-compliant')
        
        for name3, f_id in zip(df2['Name'], df2['FFOP']):
            if bar == name3:
                if f_id == 1.0:
                    FF_ID.append('Compliant')
                else:
                    FF_ID.append('Non-compliant')
                
    else:
        FF_ID.append('Non-compliant')
        sap_account.append('Non-compliant')
    
    
    if len(c)>0:
        
        df3= pd.DataFrame(c)
    
        df3.columns = ['Job Status', 'Title', 'JOBNUM', 'ows_Projects', 'ows_FunctionalGroup', 'ows_FunctionalArea', 'ows_Application', 'RANGE', 'JOBNAME', 'SAP_SYSTEM', 'ows_Created', 'ows_Job Criticality', 'ows_Job Summary', 'ows_Job Description', 'ows_Job Purpose', 'FREQUENCY', 'ows_Author', 'ows_Functional Contact', 'ows_Modified', 'ows_Editor', 'JOB_OWNER', 'ows_Job Type', 'ows_Prd_Aprd_By', 'ows_Quality_arv_by', 'ows_Restart Procedure', 'ows_Start Time', 'ows_Technical Contact', 'ows_Threshold_Ticketing_Enabled', 'ows_Time Zone', 'ows_Title', 'ows_Trigger Type', 'ows__UIVersionString', 'ows_Support Team Email', 'ows_Support Team', 'ows_Comments', 'ows_Nxt_Rec_Date', 'STEP', 'Varian_1', 'Program_1', 'ows_QADateHist', 'ows_Quality_aprd_On', 'ows_Certified Date', 'ows_Certified By', 'ows_Schedular Approved O', 'ows_StepNumber_2', 'ows_Varian_2', 'ows_Program_2', 'ows_StepNumber_10', 'ows_StepNumber_3', 'ows_StepNumber_4', 'ows_StepNumber_5', 'ows_StepNumber_6', 'ows_StepNumber_7', 'ows_StepNumber_8', 'ows_StepNumber_9', 'ows_Varian_10', 'ows_Varian_3', 'ows_Varian_4', 'ows_Varian_5', 'ows_Varian_6', 'ows_Varian_7', 'ows_Varian_8', 'ows_Varian_9', 'ows_StepNumber_11', 'ows_Varian_11', 'ows_Tigger Name', 'ows_StepNumber_12', 'ows_StepNumber_13', 'ows_StepNumber_14', 'ows_StepNumber_15', 'ows_StepNumber_16', 'ows_Varian_12', 'ows_Varian_13', 'ows_Varian_14', 'ows_Varian_15', 'ows_Varian_16', 'ows_EmailTes']
    
        for system, name, status, number in zip(df3['SAP_SYSTEM'], df3['JOBNAME'], df3['Job Status'], df3['JOBNUM']):
            if bar==name:
                systems.append(system)
                bcs_num.append(number)
                if status == 'Active':
                    statuses.append('100%')
                else:
                    statuses.append('0%')
                    
    else:
        bcs_num.append('N/A')
        systems.append('N/A')
        statuses.append('N/A')
        
    for c1, c2, c3, p2, p4 in zip(statuses, FF_ID,sap_account,valid_rate,final_average ):
        new_c1 = []
        new_c2 = []
        new_c3 = []
        new_p2 = []
        new_p4 = []
        if c1 == "100%":
            new_c1.append(20)
        else:
            new_c1.append(0)
        if c2 == "Compliant":
            new_c2.append(20)
        else:
            new_c2.append(0)
        if c3 == "Compliant":
            new_c3.append(20)
        else:
            new_c3.append(0)
        if p2 == "Yes":
            new_p2.append(20)
        else:
            new_p2.append(0)
        if p4 == 0:
            new_p4.append(0)
        elif "Runtime improvement efficiency is faster by:" in p4:
            new_p4.append(20)
        else:
            new_p4.append(0)
        
        for nc1, nc2,nc3,np2,np4 in zip(new_c1, new_c2, new_c3, new_p2, new_p4):
            pass_num = (nc1+nc2+nc3+np2+np4)/100
            final_pass.append(pass_num)
            
        
 
    for pass_nums,FF,final_system, x_run,b_num,sap,final_status, final_completion, final_valid, avg_runtime in zip(final_pass,FF_ID,systems,final_average,bcs_num,sap_account, statuses, completion_rate, valid_rate, runtimes):
            return render_template('templates.html',pass_id='{:.1%}'.format(pass_nums),FFID = '{}'.format(FF),color = 'red',x_runtime='{}'.format(x_run),BID='{}'.format(b_num),SID = '{}'.format(final_system), Jname='{}'.format(bar), Match='100%', SAPOP = '{}'.format(sap), Status = '{}'.format(final_status), Rate = '{:.1%}'.format(final_completion), Valid_rate ='{}'.format(final_valid), runtime = '{}'.format(avg_runtime))
@app.route('/get-bcs', methods = ['GET','POST'])
def all_bcs():

    bcs_number = request.form['bcs']    
    
    connection = pyhdb.connect(host = "cfgavsapp", port = 39015, user = "SYSTEM", password = "Om@ha13!")
    
    cursor0 = connection.cursor()
    cursor0.execute(("select JOBNAME from P08_CAS_MLDATA.BCS_DATA where JOBNUM = '{bcs}'" ).format(bcs = bcs_number))
    a0 = cursor0.fetchall()
    
    for num in a0:
        for n in num:
            bar = n
    
    cursor = connection.cursor()
    cursor.execute(("select jobname, sum(case when status ='F' then 1 else 0 end) as Success, sum(case when status ='A' then 1 else 0 end) as Fail  from P08_CAS_MLDATA.TBTCO where jobname ='{b}' group by jobname" ).format(b = bar))
    a = cursor.fetchall()
    
    cursora1 = connection.cursor()
    cursora1.execute(("select endtime,strttime, reldate from P08_CAS_MLDATA.TBTCO where jobname = '{e}'" ).format(e=bar))
    a1 = cursora1.fetchall()
    
    cursor1 = connection.cursor()
    cursor1.execute(("select jobname, sum(case when authcknam like '%SAPOP%' then 1 else 0 end) as SAPOP_EXIST,sum(case when authcknam not like '%SAPOP%' then 1 else 0 end) as SAPOP_NOT,sum(case when sdluname like '%FF%' then 1 else 0 end) as FF_EXIST,sum(case when sdluname not like '%FF%' then 1 else 0 end) as FF_NOT  from P08_CAS_MLDATA.TBTCP where jobname = '{c}' group by jobname" ).format(c = bar))
    b = cursor1.fetchall()
    
    cursor2 = connection.cursor()
    cursor2.execute(("select * from P08_CAS_MLDATA.BCS_DATA where jobname = '{d}'" ).format(d=bar))
    c = cursor2.fetchall()
    
    
    
    bcs_num = []
    systems = []
    statuses = []
    completion_rate = []
    valid_rate = []
    sap_account = []
    runtimes =[]
    x_runtimes=[]
    x30_runtimes = []
    final_average=[]
    
    list1 = []
    list2 = []
    list3 = []
    x_list1 =[]
    x_list2=[]
    x_list3 = []

    x30_list1 =[]
    x30_list2=[]
    x30_list3 = []
    
    FF_ID = []
    
    final_pass = []
    
    if len(a) >0:
        df1= pd.DataFrame(a)
        
        df1.rename(columns={'0':'Name', '1':'Success', '2':'Fail'})
        df1.columns=['Name', 'Success', 'Fail']
        df1['Runs'] = df1['Success'] + df1['Fail']
        
        df1['Success_rate'] = df1['Success']/df1['Runs']
        
        
        for success, name2 in zip(df1['Success_rate'], df1['Name']):
            if bar==name2:
                completion_rate.append(success)
                if success >0.8:
                    valid_rate.append('Yes')
                elif success <0.8:
                    valid_rate.append('No')
                    
    else:
        completion_rate.append(0)
        valid_rate.append('N/A')
    if len(a1)>0:
        dfa1= pd.DataFrame(a1)
        dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
        dfa1.columns=['Endtime','Startime', 'Date']
        
        dfa1['Endtime'] = dfa1['Endtime'].dropna()
        dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Endtime'].dropna(inplace= True)
        
        dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
        dfa1['Endtime'].dropna(inplace = True)
        
        dfa1['Startime'] = dfa1['Startime'].dropna()
        #df2['Endtime'] = df2['Endtime'].astype('int64')
        dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Startime'].dropna(inplace= True)
        
        dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
        dfa1['Startime'].dropna(inplace = True)
        dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
        
        means = dfa1['avg'].mean()

        mins = means/60
        hours = means/3600
        if means>60 and means <3600:
            list1.append(mins)
        else:
            list1.append(0)
        if means < 60:
            list2.append(means)
        else:
            list2.append(0)
        if means>3600:
            list3.append(hours)
        else:
            list3.append(0)
        
        for num, num1, num2 in zip(list3,list1,list2):
            runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))

        def x():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("10day")]
            
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x_list1.append(mins)
            else:
                x_list1.append(0)
            if means < 60:
                x_list2.append(means)
            else:
                x_list2.append(0)
            if means>3600:
                x_list3.append(hours)
            else:
                x_list3.append(0)
            
            for num, num1, num2 in zip(x_list3,x_list1,x_list2):
                x_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        
        x()
        def x30():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("30day")]
        
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x30_list1.append(mins)
            else:
                x30_list1.append(0)
            if means < 60:
                x30_list2.append(means)
            else:
                x30_list2.append(0)
            if means>3600:
                x30_list3.append(hours)
            else:
                x30_list3.append(0)
            
            for num, num1, num2 in zip(x30_list3,x30_list1,x30_list2):
                x30_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        x30()

        for num, num1, num2, num3, num4, num5 in zip(x_list1, x_list2,x_list3, x30_list1, x30_list2,x30_list3):
            total10 = num+num1+num2
            total30 = num3+num4+num5
            if total10>0 and total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            elif total10>0 or total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            else:
                final_average.append(0)

    else:
        final_average.append(0)
        runtimes.append(0)
        x_runtimes.append(0)
        x30_runtimes.append(0)
    if len(b)>0:
        
        df2= pd.DataFrame(b)
        
        df2.rename(columns={'0':'Name', '1':'SAP_EXIST','2':'SAP_NOT', '3':'FF_EXIST','4':'FF_NOT'})
        df2.columns=['Name', 'SAP_EXIST','SAP_NOT', 'FF_EXIST','FF_NOT']
        
        df2['SAP_TOTAL'] = df2['SAP_EXIST']+df2['SAP_NOT']
        
        df2['FF_TOTAL'] = df2['FF_EXIST'] + df2['FF_NOT']
        
        df2['SAPOP'] = df2['SAP_EXIST']/df2['SAP_TOTAL']
        
        df2['FFOP'] = df2['FF_EXIST']/df2['FF_TOTAL']
        
        for name3, sap1 in zip(df2['Name'], df2['SAPOP']):
            if bar == name3:
                if sap1 == 1.0:
                    sap_account.append('Compliant')
                else:
                    sap_account.append('Non-compliant')
        
        for name3, f_id in zip(df2['Name'], df2['FFOP']):
            if bar == name3:
                if f_id == 1.0:
                    FF_ID.append('Compliant')
                else:
                    FF_ID.append('Non-compliant')
                
    else:
        FF_ID.append('Non-compliant')
        sap_account.append('Non-compliant')
    
    
    if len(c)>0:
        
        df3= pd.DataFrame(c)
    
        df3.columns = ['Job Status', 'Title', 'JOBNUM', 'ows_Projects', 'ows_FunctionalGroup', 'ows_FunctionalArea', 'ows_Application', 'RANGE', 'JOBNAME', 'SAP_SYSTEM', 'ows_Created', 'ows_Job Criticality', 'ows_Job Summary', 'ows_Job Description', 'ows_Job Purpose', 'FREQUENCY', 'ows_Author', 'ows_Functional Contact', 'ows_Modified', 'ows_Editor', 'JOB_OWNER', 'ows_Job Type', 'ows_Prd_Aprd_By', 'ows_Quality_arv_by', 'ows_Restart Procedure', 'ows_Start Time', 'ows_Technical Contact', 'ows_Threshold_Ticketing_Enabled', 'ows_Time Zone', 'ows_Title', 'ows_Trigger Type', 'ows__UIVersionString', 'ows_Support Team Email', 'ows_Support Team', 'ows_Comments', 'ows_Nxt_Rec_Date', 'STEP', 'Varian_1', 'Program_1', 'ows_QADateHist', 'ows_Quality_aprd_On', 'ows_Certified Date', 'ows_Certified By', 'ows_Schedular Approved O', 'ows_StepNumber_2', 'ows_Varian_2', 'ows_Program_2', 'ows_StepNumber_10', 'ows_StepNumber_3', 'ows_StepNumber_4', 'ows_StepNumber_5', 'ows_StepNumber_6', 'ows_StepNumber_7', 'ows_StepNumber_8', 'ows_StepNumber_9', 'ows_Varian_10', 'ows_Varian_3', 'ows_Varian_4', 'ows_Varian_5', 'ows_Varian_6', 'ows_Varian_7', 'ows_Varian_8', 'ows_Varian_9', 'ows_StepNumber_11', 'ows_Varian_11', 'ows_Tigger Name', 'ows_StepNumber_12', 'ows_StepNumber_13', 'ows_StepNumber_14', 'ows_StepNumber_15', 'ows_StepNumber_16', 'ows_Varian_12', 'ows_Varian_13', 'ows_Varian_14', 'ows_Varian_15', 'ows_Varian_16', 'ows_EmailTes']
    
        for system, name, status, number in zip(df3['SAP_SYSTEM'], df3['JOBNAME'], df3['Job Status'], df3['JOBNUM']):
            if bar==name:
                systems.append(system)
                bcs_num.append(number)
                if status == 'Active':
                    statuses.append('100%')
                else:
                    statuses.append('0%')
                    
    else:
        bcs_num.append('N/A')
        systems.append('N/A')
        statuses.append('N/A')
        
    for c1, c2, c3, p2, p4 in zip(statuses, FF_ID,sap_account,valid_rate,final_average ):
        new_c1 = []
        new_c2 = []
        new_c3 = []
        new_p2 = []
        new_p4 = []
        if c1 == "100%":
            new_c1.append(20)
        else:
            new_c1.append(0)
        if c2 == "Compliant":
            new_c2.append(20)
        else:
            new_c2.append(0)
        if c3 == "Compliant":
            new_c3.append(20)
        else:
            new_c3.append(0)
        if p2 == "Yes":
            new_p2.append(20)
        else:
            new_p2.append(0)
        if p4 == 0:
            new_p4.append(0)
        elif "Runtime improvement efficiency is faster by:" in p4:
            new_p4.append(20)
        else:
            new_p4.append(0)
        
        for nc1, nc2,nc3,np2,np4 in zip(new_c1, new_c2, new_c3, new_p2, new_p4):
            pass_num = (nc1+nc2+nc3+np2+np4)/100
            final_pass.append(pass_num)
            
        
        
    for pass_nums,FF,final_system, x_run,b_num,sap,final_status, final_completion, final_valid, avg_runtime in zip(final_pass,FF_ID,systems,final_average,bcs_num,sap_account, statuses, completion_rate, valid_rate, runtimes):
            return render_template('templates.html',pass_id='{:.1%}'.format(pass_nums),FFID = '{}'.format(FF),color = 'red',x_runtime='{}'.format(x_run),BID='{}'.format(b_num),SID = '{}'.format(final_system), Jname='{}'.format(bar), Match='100%', SAPOP = '{}'.format(sap), Status = '{}'.format(final_status), Rate = '{:.1%}'.format(final_completion), Valid_rate ='{}'.format(final_valid), runtime = '{}'.format(avg_runtime))

@app.route('/get-bcs/<key>', methods = ['GET','POST'])
def get_bcs(key):

    bcs_number = key
    
    connection = pyhdb.connect(host = "cfgavsapp", port = 39015, user = "SYSTEM", password = "Om@ha13!")
    
    cursor0 = connection.cursor()
    cursor0.execute(("select JOBNAME from P08_CAS_MLDATA.BCS_DATA where JOBNUM = '{bcs}'" ).format(bcs = bcs_number))
    a0 = cursor0.fetchall()
    
    for num in a0:
        for n in num:
            bar = n
    
    cursor = connection.cursor()
    cursor.execute(("select jobname, sum(case when status ='F' then 1 else 0 end) as Success, sum(case when status ='A' then 1 else 0 end) as Fail  from P08_CAS_MLDATA.TBTCO where jobname ='{b}' group by jobname" ).format(b = bar))
    a = cursor.fetchall()
    
    cursora1 = connection.cursor()
    cursora1.execute(("select endtime,strttime, reldate from P08_CAS_MLDATA.TBTCO where jobname = '{e}'" ).format(e=bar))
    a1 = cursora1.fetchall()
    
    cursor1 = connection.cursor()
    cursor1.execute(("select jobname, sum(case when authcknam like '%SAPOP%' then 1 else 0 end) as SAPOP_EXIST,sum(case when authcknam not like '%SAPOP%' then 1 else 0 end) as SAPOP_NOT,sum(case when sdluname like '%FF%' then 1 else 0 end) as FF_EXIST,sum(case when sdluname not like '%FF%' then 1 else 0 end) as FF_NOT  from P08_CAS_MLDATA.TBTCP where jobname = '{c}' group by jobname" ).format(c = bar))
    b = cursor1.fetchall()
    
    cursor2 = connection.cursor()
    cursor2.execute(("select * from P08_CAS_MLDATA.BCS_DATA where jobname = '{d}'" ).format(d=bar))
    c = cursor2.fetchall()
    
    
    
    bcs_num = []
    systems = []
    statuses = []
    completion_rate = []
    valid_rate = []
    sap_account = []
    runtimes =[]
    x_runtimes=[]
    x30_runtimes = []
    final_average=[]
    
    list1 = []
    list2 = []
    list3 = []
    x_list1 =[]
    x_list2=[]
    x_list3 = []

    x30_list1 =[]
    x30_list2=[]
    x30_list3 = []
    
    FF_ID = []
    
    final_pass = []
    
    if len(a) >0:
        df1= pd.DataFrame(a)
        
        df1.rename(columns={'0':'Name', '1':'Success', '2':'Fail'})
        df1.columns=['Name', 'Success', 'Fail']
        df1['Runs'] = df1['Success'] + df1['Fail']
        
        df1['Success_rate'] = df1['Success']/df1['Runs']
        
        
        for success, name2 in zip(df1['Success_rate'], df1['Name']):
            if bar==name2:
                completion_rate.append(success)
                if success >0.8:
                    valid_rate.append('Yes')
                elif success <0.8:
                    valid_rate.append('No')
                    
    else:
        completion_rate.append(0)
        valid_rate.append('N/A')
    if len(a1)>0:
        dfa1= pd.DataFrame(a1)
        dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
        dfa1.columns=['Endtime','Startime', 'Date']
        
        dfa1['Endtime'] = dfa1['Endtime'].dropna()
        dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Endtime'].dropna(inplace= True)
        
        dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
        dfa1['Endtime'].dropna(inplace = True)
        
        dfa1['Startime'] = dfa1['Startime'].dropna()
        #df2['Endtime'] = df2['Endtime'].astype('int64')
        dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
        dfa1['Startime'].dropna(inplace= True)
        
        dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
        dfa1['Startime'].dropna(inplace = True)
        dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
        
        means = dfa1['avg'].mean()

        mins = means/60
        hours = means/3600
        if means>60 and means <3600:
            list1.append(mins)
        else:
            list1.append(0)
        if means < 60:
            list2.append(means)
        else:
            list2.append(0)
        if means>3600:
            list3.append(hours)
        else:
            list3.append(0)
        
        for num, num1, num2 in zip(list3,list1,list2):
            runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))

        def x():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("10day")]
            
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x_list1.append(mins)
            else:
                x_list1.append(0)
            if means < 60:
                x_list2.append(means)
            else:
                x_list2.append(0)
            if means>3600:
                x_list3.append(hours)
            else:
                x_list3.append(0)
            
            for num, num1, num2 in zip(x_list3,x_list1,x_list2):
                x_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        
        x()
        def x30():
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
            dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("30day")]
        
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                x30_list1.append(mins)
            else:
                x30_list1.append(0)
            if means < 60:
                x30_list2.append(means)
            else:
                x30_list2.append(0)
            if means>3600:
                x30_list3.append(hours)
            else:
                x30_list3.append(0)
            
            for num, num1, num2 in zip(x30_list3,x30_list1,x30_list2):
                x30_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
        x30()

        for num, num1, num2, num3, num4, num5 in zip(x_list1, x_list2,x_list3, x30_list1, x30_list2,x30_list3):
            total10 = num+num1+num2
            total30 = num3+num4+num5
            if total10>0 and total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            elif total10>0 or total30>0:
                if total10>total30:
                    final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                elif total10<total30:
                    final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                else:
                    final_average.append(0)
            else:
                final_average.append(0)

    else:
        final_average.append(0)
        runtimes.append(0)
        x_runtimes.append(0)
        x30_runtimes.append(0)
    if len(b)>0:
        
        df2= pd.DataFrame(b)
        
        df2.rename(columns={'0':'Name', '1':'SAP_EXIST','2':'SAP_NOT', '3':'FF_EXIST','4':'FF_NOT'})
        df2.columns=['Name', 'SAP_EXIST','SAP_NOT', 'FF_EXIST','FF_NOT']
        
        df2['SAP_TOTAL'] = df2['SAP_EXIST']+df2['SAP_NOT']
        
        df2['FF_TOTAL'] = df2['FF_EXIST'] + df2['FF_NOT']
        
        df2['SAPOP'] = df2['SAP_EXIST']/df2['SAP_TOTAL']
        
        df2['FFOP'] = df2['FF_EXIST']/df2['FF_TOTAL']
        
        for name3, sap1 in zip(df2['Name'], df2['SAPOP']):
            if bar == name3:
                if sap1 == 1.0:
                    sap_account.append('Compliant')
                else:
                    sap_account.append('Non-compliant')
        
        for name3, f_id in zip(df2['Name'], df2['FFOP']):
            if bar == name3:
                if f_id == 1.0:
                    FF_ID.append('Compliant')
                else:
                    FF_ID.append('Non-compliant')
                
    else:
        FF_ID.append('Non-compliant')
        sap_account.append('Non-compliant')
    
    
    if len(c)>0:
        
        df3= pd.DataFrame(c)
    
        df3.columns = ['Job Status', 'Title', 'JOBNUM', 'ows_Projects', 'ows_FunctionalGroup', 'ows_FunctionalArea', 'ows_Application', 'RANGE', 'JOBNAME', 'SAP_SYSTEM', 'ows_Created', 'ows_Job Criticality', 'ows_Job Summary', 'ows_Job Description', 'ows_Job Purpose', 'FREQUENCY', 'ows_Author', 'ows_Functional Contact', 'ows_Modified', 'ows_Editor', 'JOB_OWNER', 'ows_Job Type', 'ows_Prd_Aprd_By', 'ows_Quality_arv_by', 'ows_Restart Procedure', 'ows_Start Time', 'ows_Technical Contact', 'ows_Threshold_Ticketing_Enabled', 'ows_Time Zone', 'ows_Title', 'ows_Trigger Type', 'ows__UIVersionString', 'ows_Support Team Email', 'ows_Support Team', 'ows_Comments', 'ows_Nxt_Rec_Date', 'STEP', 'Varian_1', 'Program_1', 'ows_QADateHist', 'ows_Quality_aprd_On', 'ows_Certified Date', 'ows_Certified By', 'ows_Schedular Approved O', 'ows_StepNumber_2', 'ows_Varian_2', 'ows_Program_2', 'ows_StepNumber_10', 'ows_StepNumber_3', 'ows_StepNumber_4', 'ows_StepNumber_5', 'ows_StepNumber_6', 'ows_StepNumber_7', 'ows_StepNumber_8', 'ows_StepNumber_9', 'ows_Varian_10', 'ows_Varian_3', 'ows_Varian_4', 'ows_Varian_5', 'ows_Varian_6', 'ows_Varian_7', 'ows_Varian_8', 'ows_Varian_9', 'ows_StepNumber_11', 'ows_Varian_11', 'ows_Tigger Name', 'ows_StepNumber_12', 'ows_StepNumber_13', 'ows_StepNumber_14', 'ows_StepNumber_15', 'ows_StepNumber_16', 'ows_Varian_12', 'ows_Varian_13', 'ows_Varian_14', 'ows_Varian_15', 'ows_Varian_16', 'ows_EmailTes']
    
        for system, name, status, number in zip(df3['SAP_SYSTEM'], df3['JOBNAME'], df3['Job Status'], df3['JOBNUM']):
            if bar==name:
                systems.append(system)
                bcs_num.append(number)
                if status == 'Active':
                    statuses.append('100%')
                else:
                    statuses.append('0%')
                    
    else:
        bcs_num.append('N/A')
        systems.append('N/A')
        statuses.append('N/A')
        
    for c1, c2, c3, p2, p4 in zip(statuses, FF_ID,sap_account,valid_rate,final_average ):
        new_c1 = []
        new_c2 = []
        new_c3 = []
        new_p2 = []
        new_p4 = []
        if c1 == "100%":
            new_c1.append(20)
        else:
            new_c1.append(0)
        if c2 == "Compliant":
            new_c2.append(20)
        else:
            new_c2.append(0)
        if c3 == "Compliant":
            new_c3.append(20)
        else:
            new_c3.append(0)
        if p2 == "Yes":
            new_p2.append(20)
        else:
            new_p2.append(0)
        if p4 == 0:
            new_p4.append(0)
        elif "Runtime improvement efficiency is faster by:" in p4:
            new_p4.append(20)
        else:
            new_p4.append(0)
        
        for nc1, nc2,nc3,np2,np4 in zip(new_c1, new_c2, new_c3, new_p2, new_p4):
            pass_num = (nc1+nc2+nc3+np2+np4)/100
            final_pass.append(pass_num)
            
        
        
    for pass_nums,FF,final_system, x_run,b_num,sap,final_status, final_completion, final_valid, avg_runtime in zip(final_pass,FF_ID,systems,final_average,bcs_num,sap_account, statuses, completion_rate, valid_rate, runtimes):
            return render_template('templates.html',pass_id='{:.1%}'.format(pass_nums),FFID = '{}'.format(FF),color = 'red',x_runtime='{}'.format(x_run),BID='{}'.format(b_num),SID = '{}'.format(final_system), Jname='{}'.format(bar), Match='100%', SAPOP = '{}'.format(sap), Status = '{}'.format(final_status), Rate = '{:.1%}'.format(final_completion), Valid_rate ='{}'.format(final_valid), runtime = '{}'.format(avg_runtime))
    
@app.route('/all', methods = ['GET','POST'])
def alle():
    return render_template('templates1.html')

@app.route('/all-results', methods = ['GET','POST'])
def all_results():
    status = request.form['status']
    projects = request.form['projects']
    funcgroup = request.form['Functionalgroup']
    funcarea = request.form['Functionalarea']
    
    connection = pyhdb.connect(host = "cfgavsapp", port = 39015, user = "SYSTEM", password = "Om@ha13!")
    
    cursor2 = connection.cursor()
    cursor2.execute(("select * from P08_CAS_MLDATA.BCS_DATA" ))
    c = cursor2.fetchall()
    
    df3= pd.DataFrame(c)
        
    df3.columns = ['Job Status', 'Title', 'JOBNUM', 'ows_Projects', 'ows_FunctionalGroup', 'ows_FunctionalArea', 'ows_Application', 'RANGE', 'JOBNAME', 'SAP_SYSTEM', 'ows_Created', 'ows_Job Criticality', 'ows_Job Summary', 'ows_Job Description', 'ows_Job Purpose', 'FREQUENCY', 'ows_Author', 'ows_Functional Contact', 'ows_Modified', 'ows_Editor', 'JOB_OWNER', 'ows_Job Type', 'ows_Prd_Aprd_By', 'ows_Quality_arv_by', 'ows_Restart Procedure', 'ows_Start Time', 'ows_Technical Contact', 'ows_Threshold_Ticketing_Enabled', 'ows_Time Zone', 'ows_Title', 'ows_Trigger Type', 'ows__UIVersionString', 'ows_Support Team Email', 'ows_Support Team', 'ows_Comments', 'ows_Nxt_Rec_Date', 'STEP', 'Varian_1', 'Program_1', 'ows_QADateHist', 'ows_Quality_aprd_On', 'ows_Certified Date', 'ows_Certified By', 'ows_Schedular Approved O', 'ows_StepNumber_2', 'ows_Varian_2', 'ows_Program_2', 'ows_StepNumber_10', 'ows_StepNumber_3', 'ows_StepNumber_4', 'ows_StepNumber_5', 'ows_StepNumber_6', 'ows_StepNumber_7', 'ows_StepNumber_8', 'ows_StepNumber_9', 'ows_Varian_10', 'ows_Varian_3', 'ows_Varian_4', 'ows_Varian_5', 'ows_Varian_6', 'ows_Varian_7', 'ows_Varian_8', 'ows_Varian_9', 'ows_StepNumber_11', 'ows_Varian_11', 'ows_Tigger Name', 'ows_StepNumber_12', 'ows_StepNumber_13', 'ows_StepNumber_14', 'ows_StepNumber_15', 'ows_StepNumber_16', 'ows_Varian_12', 'ows_Varian_13', 'ows_Varian_14', 'ows_Varian_15', 'ows_Varian_16', 'ows_EmailTes']
    
    df3 = df3.rename(columns={'Job Status':'job_status'})
    df3['ows_FunctionalGroup']=df3['ows_FunctionalGroup'].replace('Sales & Service', 'Sales and Service')
    df3['ows_FunctionalArea']=df3['ows_FunctionalArea'].replace('Q&R for Design Manufacturing Service Vendors', 'QandR for Design Manufacturing Service Vendors')
    
    a = "{}".format(status)
    b= "{}".format(projects)
    c="{}".format(funcgroup)
    d="{}".format(funcarea)
    
    if status != "" and projects != "" and funcgroup != "" and funcarea != "":
        df4 = df3[(df3['job_status'] == '{}'.format(a)) & (df3['ows_Projects'] == '{}'.format(b))& (df3['ows_FunctionalGroup'] == '{}'.format(c)) &(df3['ows_FunctionalArea'] == '{}'.format(d))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status != "" and projects != "" and funcgroup!= "":
        df4 = df3[(df3['job_status'] == '{}'.format(a)) & (df3['ows_Projects'] == '{}'.format(b))& (df3['ows_FunctionalGroup'] == '{}'.format(c))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif projects != "" and funcgroup!="" and funcarea !="":
        df4 = df3[(df3['ows_Projects'] == '{}'.format(b))& (df3['ows_FunctionalGroup'] == '{}'.format(c)) &(df3['ows_FunctionalArea'] == '{}'.format(d))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status!= "" and projects != "" and funcarea != "":
        df4 = df3[(df3['job_status'] == '{}'.format(a)) & (df3['ows_Projects'] == '{}'.format(b)) &(df3['ows_FunctionalArea'] == '{}'.format(d))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status != "" and funcgroup!= "" and funcarea != "":
        df4 = df3[(df3['job_status'] == '{}'.format(a)) & (df3['ows_FunctionalGroup'] == '{}'.format(c)) &(df3['ows_FunctionalArea'] == '{}'.format(d))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status != "" and projects !="":
        df4 = df3[(df3['job_status'] == '{}'.format(a)) & (df3['ows_Projects'] == '{}'.format(b))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status!= "" and funcgroup!= "":
        df4 = df3[(df3['job_status'] == '{}'.format(a)) & (df3['ows_FunctionalGroup'] == '{}'.format(c))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status!= "" and funcarea != "":
        df4 = df3[(df3['job_status'] == '{}'.format(a))  &(df3['ows_FunctionalArea'] == '{}'.format(d))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif projects != "" and funcgroup!="":
        df4 = df3[ (df3['ows_Projects'] == '{}'.format(b))& (df3['ows_FunctionalGroup'] == '{}'.format(c)) ]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif projects != "" and funcarea !="":
        df4 = df3[(df3['ows_Projects'] == '{}'.format(b))& (df3['ows_FunctionalArea'] == '{}'.format(d))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif funcgroup!= "" and funcarea != "":
        df4 = df3[ (df3['ows_FunctionalGroup'] == '{}'.format(c)) &(df3['ows_FunctionalArea'] == '{}'.format(d))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status != "":
        df4 = df3[(df3['job_status'] == '{}'.format(a))]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif projects != "":
        df4 = df3[(df3['ows_Projects']) =='{}'.format(b)]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif funcgroup != "":
        df4 = df3[(df3['ows_FunctionalGroup']) == '{}'.format(c)]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif funcarea != "":
        df4 = df3[(df3['ows_FunctionalArea']) == '{}'.format(d)]
        df4['new_jobs'] = df4['JOBNAME']
        df4['JOBNUM']= df4['JOBNUM']
    elif status == "" and projects == "" and funcgroup == "" and funcarea == "":
        df4['new_jobs'] = df3['JOBNAME']
        df4['JOBNUM']= df3['JOBNUM']
        
    new_jobs = []
    final_pass_result = []
    bcs_number = []

    for bar in df4['new_jobs']:
        connection = pyhdb.connect(host = "cfgavsapp", port = 39015, user = "SYSTEM", password = "Om@ha13!")
    
        cursor = connection.cursor()
        cursor.execute(("select jobname, sum(case when status ='F' then 1 else 0 end) as Success, sum(case when status ='A' then 1 else 0 end) as Fail  from P08_CAS_MLDATA.TBTCO where jobname ='{b}' group by jobname" ).format(b = bar))
        a = cursor.fetchall()
        
        cursora1 = connection.cursor()
        cursora1.execute(("select endtime,strttime, reldate from P08_CAS_MLDATA.TBTCO where jobname = '{e}'" ).format(e=bar))
        a1 = cursora1.fetchall()
        
        cursor1 = connection.cursor()
        cursor1.execute(("select jobname, sum(case when authcknam like '%SAPOP%' then 1 else 0 end) as SAPOP_EXIST,sum(case when authcknam not like '%SAPOP%' then 1 else 0 end) as SAPOP_NOT,sum(case when sdluname like '%FF%' then 1 else 0 end) as FF_EXIST,sum(case when sdluname not like '%FF%' then 1 else 0 end) as FF_NOT  from P08_CAS_MLDATA.TBTCP where jobname = '{c}' group by jobname" ).format(c = bar))
        b = cursor1.fetchall()
        
        cursor2 = connection.cursor()
        cursor2.execute(("select * from P08_CAS_MLDATA.BCS_DATA where jobname = '{d}'" ).format(d=bar))
        c = cursor2.fetchall()
        
        bcs_num = []
        systems = []
        statuses = []
        completion_rate = []
        valid_rate = []
        sap_account = []
        runtimes =[]
        x_runtimes=[]
        x30_runtimes = []
        final_average=[]
        
        list1 = []
        list2 = []
        list3 = []
        x_list1 =[]
        x_list2=[]
        x_list3 = []
    
        x30_list1 =[]
        x30_list2=[]
        x30_list3 = []
        
        FF_ID = []
        
        final_pass = []
    
        
        if len(a) >0:
            df1= pd.DataFrame(a)
            
            df1.rename(columns={'0':'Name', '1':'Success', '2':'Fail'})
            df1.columns=['Name', 'Success', 'Fail']
            df1['Runs'] = df1['Success'] + df1['Fail']
            
            df1['Success_rate'] = df1['Success']/df1['Runs']
            
            
            for success, name2 in zip(df1['Success_rate'], df1['Name']):
                if bar==name2:
                    completion_rate.append(success)
                    if success >0.8:
                        valid_rate.append('Yes')
                    elif success <0.8:
                        valid_rate.append('No')
                        
        else:
            completion_rate.append(0)
            valid_rate.append('N/A')
        if len(a1)>0:
            dfa1= pd.DataFrame(a1)
            dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
            dfa1.columns=['Endtime','Startime', 'Date']
            
            dfa1['Endtime'] = dfa1['Endtime'].dropna()
            dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Endtime'].dropna(inplace= True)
            
            dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
            dfa1['Endtime'].dropna(inplace = True)
            
            dfa1['Startime'] = dfa1['Startime'].dropna()
            #df2['Endtime'] = df2['Endtime'].astype('int64')
            dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
            dfa1['Startime'].dropna(inplace= True)
            
            dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
            dfa1['Startime'].dropna(inplace = True)
            dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
            
            means = dfa1['avg'].mean()
    
            mins = means/60
            hours = means/3600
            if means>60 and means <3600:
                list1.append(mins)
            else:
                list1.append(0)
            if means < 60:
                list2.append(means)
            else:
                list2.append(0)
            if means>3600:
                list3.append(hours)
            else:
                list3.append(0)
            
            for num, num1, num2 in zip(list3,list1,list2):
                runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
    
            def x():
                dfa1= pd.DataFrame(a1)
                dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
                dfa1.columns=['Endtime','Startime', 'Date']
                dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
                dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("10day")]
                
                dfa1['Endtime'] = dfa1['Endtime'].dropna()
                dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
                dfa1['Endtime'].dropna(inplace= True)
                
                dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
                dfa1['Endtime'].dropna(inplace = True)
                
                dfa1['Startime'] = dfa1['Startime'].dropna()
                #df2['Endtime'] = df2['Endtime'].astype('int64')
                dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
                dfa1['Startime'].dropna(inplace= True)
                
                dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
                dfa1['Startime'].dropna(inplace = True)
                dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
                
                means = dfa1['avg'].mean()
        
                mins = means/60
                hours = means/3600
                if means>60 and means <3600:
                    x_list1.append(mins)
                else:
                    x_list1.append(0)
                if means < 60:
                    x_list2.append(means)
                else:
                    x_list2.append(0)
                if means>3600:
                    x_list3.append(hours)
                else:
                    x_list3.append(0)
                
                for num, num1, num2 in zip(x_list3,x_list1,x_list2):
                    x_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
            
            x()
            def x30():
                dfa1= pd.DataFrame(a1)
                dfa1.rename(columns={'0':'Endtime', '1':'Startime', '2':'Date'})
                dfa1.columns=['Endtime','Startime', 'Date']
                dfa1['Date'] = pd.to_datetime(dfa1['Date']).apply(lambda x:x.date())
                dfa1 = dfa1[dfa1['Date'] > datetime.date.today() - pd.to_timedelta("30day")]
            
                dfa1['Endtime'] = dfa1['Endtime'].dropna()
                dfa1['Endtime'] = dfa1['Endtime'].replace(r'^\s*$', np.nan, regex=True)
                dfa1['Endtime'].dropna(inplace= True)
                
                dfa1['Endtime'] = dfa1['Endtime'].astype(np.int64)
                dfa1['Endtime'].dropna(inplace = True)
                
                dfa1['Startime'] = dfa1['Startime'].dropna()
                #df2['Endtime'] = df2['Endtime'].astype('int64')
                dfa1['Startime'] = dfa1['Startime'].replace(r'^\s*$', np.nan, regex=True)
                dfa1['Startime'].dropna(inplace= True)
                
                dfa1['Startime'] = dfa1['Startime'].astype(np.int64)
                dfa1['Startime'].dropna(inplace = True)
                dfa1['avg'] = dfa1['Endtime'] - dfa1['Startime']
                
                means = dfa1['avg'].mean()
        
                mins = means/60
                hours = means/3600
                if means>60 and means <3600:
                    x30_list1.append(mins)
                else:
                    x30_list1.append(0)
                if means < 60:
                    x30_list2.append(means)
                else:
                    x30_list2.append(0)
                if means>3600:
                    x30_list3.append(hours)
                else:
                    x30_list3.append(0)
                
                for num, num1, num2 in zip(x30_list3,x30_list1,x30_list2):
                    x30_runtimes.append(("Average runtime is {d} hours and {e} minutes and {f} seconds").format(d=round(num,3),e=round(num1,3), f=round(num2,3)))
            x30()
            
            for num, num1, num2, num3, num4, num5 in zip(x_list1, x_list2,x_list3, x30_list1, x30_list2,x30_list3):
                total10 = num+num1+num2
                total30 = num3+num4+num5
                if total10>0 and total30>0:
                    if total10>total30:
                        final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                    elif total10<total30:
                        final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                    else:
                        final_average.append(0)
                elif total10>0 or total30>0:
                    if total10>total30:
                        final_average.append("Runtime improvement efficiency is slower by: {}%".format( round(100-((total10/total30)*100),3)))
                    elif total10<total30:
                        final_average.append("Runtime improvement efficiency is faster by: {}%".format( round(100-((total10/total30)*100),3)))
                    else:
                        final_average.append(0)
                else:
                    final_average.append(0)
    
        else:
            final_average.append(0)
            runtimes.append(0)
            x_runtimes.append(0)
            x30_runtimes.append(0)
        if len(b)>0:
            
            df2= pd.DataFrame(b)
            
            df2.rename(columns={'0':'Name', '1':'SAP_EXIST','2':'SAP_NOT', '3':'FF_EXIST','4':'FF_NOT'})
            df2.columns=['Name', 'SAP_EXIST','SAP_NOT', 'FF_EXIST','FF_NOT']
            
            df2['SAP_TOTAL'] = df2['SAP_EXIST']+df2['SAP_NOT']
            
            df2['FF_TOTAL'] = df2['FF_EXIST'] + df2['FF_NOT']
            
            df2['SAPOP'] = df2['SAP_EXIST']/df2['SAP_TOTAL']
            
            df2['FFOP'] = df2['FF_EXIST']/df2['FF_TOTAL']
            
            for name3, sap1 in zip(df2['Name'], df2['SAPOP']):
                if bar == name3:
                    if sap1 == 1.0:
                        sap_account.append('Compliant')
                    else:
                        sap_account.append('Non-compliant')
            
            for name3, f_id in zip(df2['Name'], df2['FFOP']):
                if bar == name3:
                    if f_id == 1.0:
                        FF_ID.append('Compliant')
                    else:
                        FF_ID.append('Non-compliant')
                    
        else:
            FF_ID.append('Non-compliant')
            sap_account.append('Non-compliant')
        
        
        if len(c)>0:
            
            df3= pd.DataFrame(c)
        
            df3.columns = ['Job Status', 'Title', 'JOBNUM', 'ows_Projects', 'ows_FunctionalGroup', 'ows_FunctionalArea', 'ows_Application', 'RANGE', 'JOBNAME', 'SAP_SYSTEM', 'ows_Created', 'ows_Job Criticality', 'ows_Job Summary', 'ows_Job Description', 'ows_Job Purpose', 'FREQUENCY', 'ows_Author', 'ows_Functional Contact', 'ows_Modified', 'ows_Editor', 'JOB_OWNER', 'ows_Job Type', 'ows_Prd_Aprd_By', 'ows_Quality_arv_by', 'ows_Restart Procedure', 'ows_Start Time', 'ows_Technical Contact', 'ows_Threshold_Ticketing_Enabled', 'ows_Time Zone', 'ows_Title', 'ows_Trigger Type', 'ows__UIVersionString', 'ows_Support Team Email', 'ows_Support Team', 'ows_Comments', 'ows_Nxt_Rec_Date', 'STEP', 'Varian_1', 'Program_1', 'ows_QADateHist', 'ows_Quality_aprd_On', 'ows_Certified Date', 'ows_Certified By', 'ows_Schedular Approved O', 'ows_StepNumber_2', 'ows_Varian_2', 'ows_Program_2', 'ows_StepNumber_10', 'ows_StepNumber_3', 'ows_StepNumber_4', 'ows_StepNumber_5', 'ows_StepNumber_6', 'ows_StepNumber_7', 'ows_StepNumber_8', 'ows_StepNumber_9', 'ows_Varian_10', 'ows_Varian_3', 'ows_Varian_4', 'ows_Varian_5', 'ows_Varian_6', 'ows_Varian_7', 'ows_Varian_8', 'ows_Varian_9', 'ows_StepNumber_11', 'ows_Varian_11', 'ows_Tigger Name', 'ows_StepNumber_12', 'ows_StepNumber_13', 'ows_StepNumber_14', 'ows_StepNumber_15', 'ows_StepNumber_16', 'ows_Varian_12', 'ows_Varian_13', 'ows_Varian_14', 'ows_Varian_15', 'ows_Varian_16', 'ows_EmailTes']
        
            for system, name, status, number in zip(df3['SAP_SYSTEM'], df3['JOBNAME'], df3['Job Status'], df3['JOBNUM']):
                if bar==name:
                    systems.append(system)
                    bcs_num.append(number)
                    if status == 'Active':
                        statuses.append('100%')
                    else:
                        statuses.append('0%')
                        
        else:
            bcs_num.append('N/A')
            systems.append('N/A')
            statuses.append('N/A')
            
        for c1, c2, c3, p2, p4 in zip(statuses, FF_ID,sap_account,valid_rate,final_average ):
            new_c1 = []
            new_c2 = []
            new_c3 = []
            new_p2 = []
            new_p4 = []
            if c1 == "100%":
                new_c1.append(20)
            else:
                new_c1.append(0)
            if c2 == "Compliant":
                new_c2.append(20)
            else:
                new_c2.append(0)
            if c3 == "Compliant":
                new_c3.append(20)
            else:
                new_c3.append(0)
            if p2 == "Yes":
                new_p2.append(20)
            else:
                new_p2.append(0)
            if p4 == 0:
                new_p4.append(0)
            elif "Runtime improvement efficiency is faster by:" in p4:
                new_p4.append(20)
            else:
                new_p4.append(0)
            
            for nc1, nc2,nc3,np2,np4 in zip(new_c1, new_c2, new_c3, new_p2, new_p4):
                pass_num = (nc1+nc2+nc3+np2+np4)/100
                final_pass.append('{:.1%}'.format(pass_num))
                
        
        for value1, value2 in zip(bcs_num, final_pass):  
            new_jobs.append(bar)
            bcs_number.append(value1)
            final_pass_result.append(value2)
            
    zipped_data = zip(new_jobs, bcs_number, final_pass_result)
    return render_template('all-results.html',value1 = zipped_data)
    
def open_browser():
    webbrowser.open_new('http://127.0.0.1:5000')

    #connection.close()
if __name__ == "__main__":
    Timer(1, open_browser).start();
    app.run(port = port)
