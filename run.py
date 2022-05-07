from __future__ import print_function
import pickle
from googleapiclient.discovery import build
import pyodbc
import pandas as pd
import logging
import time

# logging config
logging.basicConfig(filename='./logs/emailgroup.log',level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')


groupEmailYO = '*GROUPEMAILADDRESS*'

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://apps-apis.google.com/a/feeds/groups/',
          'https://www.googleapis.com/auth/admin.directory.group',
          'https://www.googleapis.com/auth/admin.directory.group.member']


def GetMembers():
    ListofEmails = []



    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)


    service = build('admin', 'directory_v1', credentials=creds, cache_discovery=False)

    groupEmail = '*GROUPEMAILADDRESS*'

    results = service.members().list(groupKey=groupEmail, fields="nextPageToken, members(email)").execute()


    if results:
        for x in results['members']:
            #print(x['email'])
            ListofEmails.append(x['email'])
    else:
        pass

    
    token = results.get('nextPageToken', None)


    while token != None:
        results = service.members().list(groupKey=groupEmail, pageToken=token, fields="nextPageToken, members(email)").execute()

        token = results.get('nextPageToken', None)


        if results:

            for x in results['members']:
                # print(x['email'])
                ListofEmails.append(x['email'])
        else:
            pass


    return ListofEmails



def DeleteMemeberfromGroup(userEmail):

    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
    service = build('admin', 'directory_v1', credentials=creds, cache_discovery=False)

    groupEmail = '*GROUPEMAILADDRESS*'


    DataReturn = service.members().delete(groupKey=groupEmail,memberKey=userEmail).execute()

    return DataReturn


def AddMemeberfromGroup(userEmail):

    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
    service = build('admin', 'directory_v1', credentials=creds, cache_discovery=False)

    groupEmail = '*GROUPEMAILADDRESS*'

    dataSET = {
        "email": userEmail
    }


    DataReturn = service.members().insert(groupKey=groupEmail,body=dataSET).execute()

    return DataReturn


def GetStudents():
    try:
        servername = '*SERVER*'
        userid = '*USERNAME*'
        password = '*PASSWORD*'
        databasename = '*DATABASE*'

        query = '''
select PEOPLE_ID,ACADEMIC_YEAR,ACADEMIC_TERM,ACADEMIC_SESSION,EVENT_ID,SECTION,EVENT_TYPE
from TRANSCRIPTDETAIL 
where ACADEMIC_YEAR = '2022' and ACADEMIC_TERM = 'SPRING'
and
PEOPLE_ID in (

select PEOPLE_ORG_ID 
from CHARGECREDIT 
where 
ACADEMIC_YEAR = '2022'
and ACADEMIC_TERM = 'SPRING'
and CHARGE_CREDIT_CODE in (
select  CODE_VALUE from CODE_CHARGECREDIT where LONG_DESC like '%online%' and [STATUS] = 'A')
GROUP BY PEOPLE_ORG_ID



)

        '''

        conn = pyodbc.connect('Driver={SQL Server};Server='+servername+  ';UID='+userid+';PWD='+password+';Database='+databasename)

        df = pd.read_sql_query(query, conn)


        DataToReturn = df.to_dict('records')


        return DataToReturn

    except Exception as e:
         print(e)


def GetStudentEmail(studentid):
    try:
        servername = '*SERVER*'
        userid = '*USERNAME*'
        password = '*PASSWORD*'
        databasename = '*DATABASE*'

        query = '''
select Email from EmailAddress where PeopleOrgId = '{studentid}'
and EmailType = 'CAMP' and IsActive = '1'

        '''.format(studentid=studentid)

        conn = pyodbc.connect('Driver={SQL Server};Server='+servername+  ';UID='+userid+';PWD='+password+';Database='+databasename)

        df = pd.read_sql_query(query, conn)

        DataToReturn = df.to_dict('records')

        return DataToReturn[0]['Email']

    except Exception as e:
         print(e)


def GetAllStudents():
    try:
        servername = '*SERVER*'
        userid = '*USERNAME*'
        password = '*PASSWORD*'
        databasename = '*DATABASE*'

        query = '''
select PEOPLE_ID from ACADEMIC 
where ACADEMIC_YEAR = '2022'
and ACADEMIC_TERM = 'SPRING'
and ACADEMIC_SESSION = ''
and PRIMARY_FLAG = 'Y'
and CREDITS > 0
and CURRICULUM <> 'HS'

        '''

        conn = pyodbc.connect('Driver={SQL Server};Server='+servername+  ';UID='+userid+';PWD='+password+';Database='+databasename)

        df = pd.read_sql_query(query, conn)

        DataToReturn = df['PEOPLE_ID'].values.tolist()


        return DataToReturn

    except Exception as e:
         print(e)


def Diff(li1, li2):
    return list(set(li1) - set(li2))

def Diff2(li1, li2):
    return list(set(li2) - set(li1))


#Empty Lists
toRemoved = []
ids = []
Emaillist = []

Data = GetStudents()

for x in Data:
    if x["EVENT_TYPE"] != "ONL":
        toRemoved.append(x["PEOPLE_ID"])


z = filter(lambda x:x['PEOPLE_ID'] not in toRemoved, Data )
OnlineOnly = list(z)

for i in OnlineOnly:
    ids.append(i["PEOPLE_ID"])

idsSorted = set(ids)

allstudents = GetAllStudents()

xdifference = Diff(allstudents,idsSorted)



for ii in xdifference:
    xemail = GetStudentEmail(ii)
    if not(xemail):
        continue
    else:
        Emaillist.append(xemail)



GsuiteData = GetMembers()
print(len(GsuiteData))

EmptyList = []

for x in GsuiteData:
    if '*SUBDOMAIN EMAIL ADDRESS*' in x:
        EmptyList.append(x)


DBEmails = Emaillist

newDATAx = Diff(EmptyList,DBEmails)

newDATAy = Diff2(DBEmails,EmptyList)

if(newDATAx):


    for xemail in newDATAx:
        DeleteMemeberfromGroup(xemail)

        logging.info("DELETED:.............." + xemail + "..............to.............." + groupEmailYO)
    else:
        logging.info("Failed To Delete:.............."+xemail + "..............to.............." + groupEmailYO)



else:
    logging.info("Nothing to delete!")


if(newDATAy):

    for yemail in newDATAy:
        try:
            AddMemeberfromGroup(yemail)


            logging.info("ADDED:.............."+yemail + "..............to.............." + groupEmailYO)
            time.sleep(1)
        except Exception as e:

            logging.info("Failed To Add:.............."+yemail + "..............to.............." + groupEmailYO)
            logging.info(e)
else:
    logging.info("Nothing to add!")

