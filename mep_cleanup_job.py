import sys
import time
from uuid import UUID
from xml.dom import minidom
import subprocess


from datetime import datetime
from cassandra.cluster import Cluster

from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.query import named_tuple_factory

profile = ExecutionProfile(
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    request_timeout=15,
    row_factory=named_tuple_factory
)

session = None
primecast_account_lookup_stmt_by_name = None
job_lookup_stmt_by_enddate_and_account= None
job_lookup_stmt_by_id= None
template_lookup_stmt_by_job= None
template_smildefinition_lookup_stmt_by_id= None
file_delete_by_id= None
template_delete_by_id= None
notification_lookup_stmt_by_job= None
notification_delete_by_id= None
parameterisedlistmsisdn_lookup_by_parameterisedlist_id= None
parameterisedlistitem_delete_by_id= None
parameterisedlistmsisdn_delete_by_parameterisedlist= None
parameterisedlist_delete_by_id= None
parameterisedlistjob_delete_by_parameterisedlist= None
flightstatushistory_lookup_by_job_id= None
flightstatushistory_lookup_by_id= None
job_delete_by_id= None
timeframe_lookup_stmt_by_id= None
timeframeday_delete_by_id= None
timeframe_delete_by_id= None
note_lookup_stmt_by_job= None
note_delete_by_id= None

def initializePreparedStatements():
    global primecast_account_lookup_stmt_by_name
    global job_lookup_stmt_by_enddate_and_account
    global job_lookup_stmt_by_id
    global template_lookup_stmt_by_job
    global template_smildefinition_lookup_stmt_by_id
    global file_delete_by_id
    global template_delete_by_id
    global notification_lookup_stmt_by_job
    global notification_delete_by_id
    global parameterisedlistmsisdn_lookup_by_parameterisedlist_id
    global parameterisedlistitem_delete_by_id
    global parameterisedlistmsisdn_delete_by_parameterisedlist
    global parameterisedlist_delete_by_id
    global parameterisedlistjob_delete_by_parameterisedlist
    global flightstatushistory_lookup_by_job_id
    global flightstatushistory_lookup_by_id
    global job_delete_by_id
    global timeframe_lookup_stmt_by_id
    global timeframeday_delete_by_id
    global timeframe_delete_by_id
    global note_lookup_stmt_by_job
    global note_delete_by_id

    primecast_account_lookup_stmt_by_name = session.prepare("SELECT id FROM mep_primecastaccount where name = ? ALLOW FILTERING")
    job_lookup_stmt_by_enddate_and_account = session.prepare("SELECT id,name FROM mep_job WHERE enddate < ? AND  account = ? \
         ALLOW FILTERING")
    job_lookup_stmt_by_id = session.prepare("SELECT id, name, parameterisedcontactlist, location, profile, timeframe FROM mep_job WHERE id = ?")
    template_lookup_stmt_by_job = session.prepare("SELECT id FROM mep_template WHERE job = ? ALLOW FILTERING")
    template_smildefinition_lookup_stmt_by_id = session.prepare("SELECT smildefinition FROM mep_template WHERE id = ?")
    file_delete_by_id = session.prepare("DELETE FROM mep_file WHERE id =?")
    template_delete_by_id = session.prepare("DELETE FROM mep_template WHERE id = ?")
    notification_lookup_stmt_by_job =  session.prepare("SELECT id FROM mep_notification WHERE flight = ? ALLOW FILTERING")
    notification_delete_by_id = session.prepare("DELETE FROM mep_notification WHERE id = ?")
    parameterisedlistmsisdn_lookup_by_parameterisedlist_id = session.prepare("SELECT parameterisedlistitem FROM mep_parameterisedlist_msisdn WHERE parameterisedlist = ?")
    parameterisedlistitem_delete_by_id = session.prepare("DELETE FROM mep_parameterisedlistitem WHERE id = ?")
    parameterisedlistmsisdn_delete_by_parameterisedlist = session.prepare("DELETE FROM mep_parameterisedlist_msisdn WHERE parameterisedlist = ?")
    parameterisedlist_delete_by_id =  session.prepare("DELETE FROM mep_parameterisedlist WHERE id = ?")
    parameterisedlistjob_delete_by_parameterisedlist = session.prepare("DELETE FROM mep_parameterisedlist_job WHERE parameterisedlist= ?")
    flightstatushistory_lookup_by_job_id = session.prepare("SELECT id FROM flightstatushistory WHERE flight = ? ALLOW FILTERING")
    flightstatushistory_lookup_by_id = session.prepare("DELETE FROM flightstatushistory WHERE id = ?")
    job_delete_by_id =  session.prepare("DELETE FROM mep_job WHERE id = ?")
    timeframe_lookup_stmt_by_id = session.prepare("SELECT id, job, monday, tuesday, wednesday, thursday, friday, saturday, sunday FROM mep_timeframe WHERE id = ?")
    timeframeday_delete_by_id = session.prepare("DELETE FROM mep_timeframeday WHERE id = ?")
    timeframe_delete_by_id = session.prepare("DELETE FROM mep_timeframe WHERE id = ?")
    note_lookup_stmt_by_job = session.prepare("SELECT id FROM  mep_note WHERE flight  = ? ALLOW FILTERING")
    note_delete_by_id = session.prepare("DELETE FROM mep_note WHERE id = ?")


def initializeCassandraSession():
    global session
    cassandra_ip_address = input("Enter Cassandra Cluster IP Address (example 192.168.123.219):\n")
    ip_list = [cassandra_ip_address]
    cassandra_keyspace = input("Enter Cassandra Keyspace (example Openmind , Openmind_BE etc):\n")
    cluster = Cluster(ip_list, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    print(f"Connecting to cassandra : {ip_list} keyspace: {cassandra_keyspace}.")
    session = cluster.connect(cassandra_keyspace)
    print("Successfully connected to Cassandra.")
    initializePreparedStatements()
    return session


def loadPrimeCastAccount():
    primecast_account_name_id_map = {}
    primecast_account_name = input("Enter PrimecastAccount Name .(example TestAccount):\n")
    primecast_account_ids = []

    primecast_account_rows = session.execute(primecast_account_lookup_stmt_by_name, [primecast_account_name])

    for primecast_account_row in primecast_account_rows:
        primecast_account_ids.append(primecast_account_row.id)

    print(f"Primecast account id {primecast_account_ids}")
    if len(primecast_account_ids) == 0:
        print(f"Account not found by name {primecast_account_name}")
        sys.exit()
    elif len(primecast_account_ids) > 1:
        print(f"Multiple Account not found by name {primecast_account_name}")
        sys.exit()
    else:
        print(f"Primecast Account found by name '{primecast_account_name}' with id ({primecast_account_ids[0]})")
    primecast_account_name_id_map[primecast_account_name] = primecast_account_ids[0]
    return primecast_account_name_id_map


def validateDate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM:SS")


def userInputDate():
    date_before_to_cleanup = input("Enter a date before all jobs should be deleted(the format should be YYYY-MM-DD HH:MM:SS example '2020-12-31 23:59:59')\n")
    validateDate(date_before_to_cleanup)
    return date_before_to_cleanup


def findJobsForCleanUp(primecast_account_id, date_before_to_cleanup):
    job_rows = session.execute(job_lookup_stmt_by_enddate_and_account, [datetime.strptime(date_before_to_cleanup, '%Y-%m-%d %H:%M:%S'), primecast_account_id])
    job_ids_to_delete = []
    for job_row in job_rows:
        print(f"Candidate for Deletion: Job name => {job_row.name}")
        job_ids_to_delete.append(job_row.id)
    return job_ids_to_delete


def getIdFromPrimecastAccountNameIdMap(primecast_account_name_id_map):
    primecast_account_id = None
    for i in primecast_account_name_id_map:
        primecast_account_id = primecast_account_name_id_map[i]
        break
    return primecast_account_id


def confirm_deletion_of_jobs():
    check = str(input("Do you want to proceed cleanup of jobs ? (Y/N): ")).lower().strip()
    try:
        if check[0] == 'y':
            return True
        elif check[0] == 'n':
            return False
        else:
            print('Invalid Input.')
            return confirm_deletion_of_jobs()
    except Exception as error:
        print("Please enter valid inputs.")
        print(error)
        return confirm_deletion_of_jobs()


def deleteMediaFiles(file_ids):
    for file_id in file_ids:
        session.execute(file_delete_by_id, [file_id])


def deleteTemplateMediaFiles(template_id):
    row = session.execute(template_smildefinition_lookup_stmt_by_id, [template_id]).one()
    smildefinition_xml = row.smildefinition
    file_ids = []

    if smildefinition_xml:
        xml_doc = minidom.parseString(smildefinition_xml)
        for element in xml_doc.getElementsByTagName('img'):
            file_ids.append(UUID(element.attributes['src'].value))
        for element in xml_doc.getElementsByTagName('video'):
            file_ids.append(UUID( element.attributes['src'].value))
        for element in xml_doc.getElementsByTagName('audio'):
            file_ids.append(UUID(element.attributes['src'].value))

    if len(file_ids) > 0:
        print(f"Found {len(file_ids)} file(s) referenced in smildefinitions to delete.")
        deleteMediaFiles(file_ids)


def deleteTemplates(template_ids):
    for template_id in template_ids:
        deleteTemplateMediaFiles(template_id)

    for template_id in template_ids:
        session.execute(template_delete_by_id, [template_id])


def deleteJobTemplates(job_dict):
    print(f"Searching Templates for job: {job_dict['name']}.")
    job_id = job_dict["id"]
    template_ids = []
    template_rows = session.execute(template_lookup_stmt_by_job, [job_id])

    for template_row in template_rows:
        template_ids.append(template_row.id)
    if len(template_ids) > 0:
        print(f"Found {len(template_ids)} Template(s) to delete.")
        deleteTemplates(template_ids)
        print(f"Successfully deleted the Template(s).")
        # time.sleep(1)
    else:
        print(f"Found {len(template_ids)} Template(s) to delete.")


def deleteNotifications(job_notification_ids):
    for job_notification_id in job_notification_ids:
        session.execute(notification_delete_by_id, [job_notification_id])


def deleteJobNotifications(job_dict):
    job_id = job_dict["id"]
    job_notification_ids = []
    # notification_rows = session.execute(f"SELECT id FROM mep_notification WHERE flight = {job_id} ALLOW FILTERING")
    notification_rows = session.execute(notification_lookup_stmt_by_job,[job_id])

    for notification_row in notification_rows:
        job_notification_ids.append(notification_row.id)

    if len(job_notification_ids) > 0:
        print(f"Found {len(job_notification_ids)} Notifications(s) to delete.")
        deleteNotifications( job_notification_ids)
        print(f"Successfully deleted the Notifications.")
        # time.sleep(1)
    else:
        print(f"Found {len(job_notification_ids)} Notifications to delete.")


def findJobById(job_id):
    row = session.execute(job_lookup_stmt_by_id,[job_id]).one()

    job_dict = {"id": row.id, "name": row.name, "parameterisedcontactlist": row.parameterisedcontactlist,
                "location": row.location, "profile": row.profile, "timeframe": row.timeframe}
    return job_dict


def deleteParameterisedListJob(parameterised_list_id):
    session.execute(parameterisedlistjob_delete_by_parameterisedlist, [parameterised_list_id])


def deleteParameterisedList(parameterised_list_id):
    session.execute(parameterisedlist_delete_by_id,[parameterised_list_id])


def deleteParameterisedIntermediate(parameterised_list_id):
    paramaterised_list_item_ids = []
    parameterisedlist_msisdn_rows = session.execute(parameterisedlistmsisdn_lookup_by_parameterisedlist_id, [parameterised_list_id])
    for parameterisedlist_msisdn_row in parameterisedlist_msisdn_rows:
        paramaterised_list_item_ids.append(parameterisedlist_msisdn_row.parameterisedlistitem)

    if len(paramaterised_list_item_ids) > 0:
        for paramaterised_list_item_id in paramaterised_list_item_ids:
            session.execute(parameterisedlistitem_delete_by_id,[paramaterised_list_item_id])
        session.execute(parameterisedlistmsisdn_delete_by_parameterisedlist, [parameterised_list_id] )


def deleteJobParameterisedList(job_dict):
    parameterised_list_id = job_dict["parameterisedcontactlist"]
    if bool(parameterised_list_id):
        print(f"Deleting parameterised list for job: {job_dict['name']}")
        print(f"Deleting parameterised in progress please wait.")
        deleteParameterisedIntermediate( parameterised_list_id)
        deleteParameterisedList( parameterised_list_id)
        deleteParameterisedListJob( parameterised_list_id)
        print(f"Successfully deleted parameterised.")

    else:
        print(f"Found no parameterised list for job: {job_dict['name']}")


def deleteFlightStatusHistory(job_dict):
    print(f"Searching flight status histories for job: {job_dict['name']}.")
    job_id = job_dict["id"]
    flightstatushistory_ids = []
    flightstatushistory_rows = session.execute(flightstatushistory_lookup_by_job_id, [job_id])

    for flightstatushistory_row in flightstatushistory_rows:
        flightstatushistory_ids.append(flightstatushistory_row.id)

    if len(flightstatushistory_ids) > 0:
        print(f"Found {len(flightstatushistory_ids)} flight status histories to delete.")
        for flightstatushistory_id in flightstatushistory_ids:
            session.execute(flightstatushistory_lookup_by_id, [flightstatushistory_id])
    else:
        print(f"Found {len(flightstatushistory_ids)} flight status histories to delete.")


def deleteJob(job_dict):
    job_id = job_dict["id"]
    print(f"Deleting job name={job_dict['name']} id={job_dict['id']}")
    session.execute(job_delete_by_id, [job_id])


def findJobTimeframeById(job_dict):
    timeframe_id = job_dict["timeframe"]
    timeframe_row = session.execute(timeframe_lookup_stmt_by_id, [timeframe_id] ).one()
    timeframe_dict = {"id": timeframe_row.id, "job": timeframe_row.job, "monday": timeframe_row.monday, "tuesday": timeframe_row.tuesday
                      , "wednesday": timeframe_row.wednesday, "thursday": timeframe_row.thursday, "friday": timeframe_row.friday,
                      "saturday": timeframe_row.saturday, "sunday": timeframe_row.sunday}
    return timeframe_dict


def deleteTimeFrameDays(timeframe_day_ids):
    for timeframe_day_id in timeframe_day_ids:
        print(f"Deleting timeframe day => id={timeframe_day_id}")
        session.execute(timeframeday_delete_by_id,[timeframe_day_id])


def deleteTimeFrameById(timeframe_id):
    session.execute(timeframe_delete_by_id, [timeframe_id] )


def deleteJobTimeFrame(job_dict):
    timeframe_id = job_dict["timeframe"]
    if timeframe_id is None:
        print(f"Found no time frame for job: {job_dict['name']}")
    else:
        timeframe_dict = findJobTimeframeById(job_dict)
        timeframe_day_ids = [timeframe_dict["monday"], timeframe_dict["tuesday"], timeframe_dict["wednesday"],
                             timeframe_dict["thursday"], timeframe_dict["saturday"], timeframe_dict["sunday"]]
        deleteTimeFrameDays(timeframe_day_ids)
        deleteTimeFrameById(timeframe_id)


def deleteJobNotes(job_dict):
    print(f"Searching notes for job: {job_dict['name']}.")
    job_id = job_dict["id"]
    note_ids = []
    note_rows = session.execute(note_lookup_stmt_by_job, [job_id])
    for note_row in note_rows:
        note_ids.append(note_row.id)

    if len(note_ids) > 0:
        print(f"Found {len(note_ids)} notes to delete.")
        for note_id in note_ids:
            session.execute(note_delete_by_id, [note_id])
    else:
        print(f"Found {len(note_ids)} notes to delete.")


def scheduleJobsForDelete(job_ids_to_delete):
    confirmation = confirm_deletion_of_jobs()
    if confirmation:
        for job_id in job_ids_to_delete:
            job_dict = findJobById(job_id)
            print(f"Executing cleanup for job: {job_dict}.")
            print("--------------------------------------")
            deleteJobTemplates(job_dict)
            deleteJobNotifications(job_dict)
            deleteJobParameterisedList(job_dict)
            deleteFlightStatusHistory(job_dict)
            deleteJobTimeFrame(job_dict)
            deleteJobNotes(job_dict)
            deleteJob(job_dict)
            time.sleep(1)
            print("--------------------------------------")


def cleanUpJobs():
    global session
    session = initializeCassandraSession()
    primecast_account_name_id_map = loadPrimeCastAccount()
    date_before_to_cleanup = userInputDate()
    primecast_account_id = getIdFromPrimecastAccountNameIdMap(primecast_account_name_id_map)
    job_ids_to_delete = findJobsForCleanUp(primecast_account_id, date_before_to_cleanup)

    if len(job_ids_to_delete) > 0:
        scheduleJobsForDelete(job_ids_to_delete)
    else:
        print("Found no jobs to delete")


cleanUpJobs()
