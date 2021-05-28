import sys
import time
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.cluster import Session

from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.query import tuple_factory

profile = ExecutionProfile(
    load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    request_timeout=15,
    row_factory=tuple_factory
)


def initializeCassandraSession():
    cassandra_ip_address = input("Enter Cassandra Cluster IP Address \n(example 192.168.123.219):\n")
    ip_list = [cassandra_ip_address]
    cassandra_keyspace = input("Enter Cassandra Keyspace \n(example Openmind , Openmind_BE etc):\n")
    cassandra_ip_address = '192.168.123.219'
    cluster = Cluster(ip_list)
    print(f"Connecting to cassandra : {cassandra_ip_address} keyspace: {cassandra_keyspace}.\n")
    session = cluster.connect(cassandra_keyspace)
    print("Successfully connected to Cassandra.\n")
    return session


def loadPrimeCastAccount(session):
    primecast_account_name_id_map = {}
    primecast_account_name = input("Enter PrimecastAccount Name \n(example TestAccount):\n")
    primecast_account_ids = []
    primecast_account_rows = session.execute(
        f"SELECT id FROM mep_primecastaccount where name ='{primecast_account_name}' ALLOW FILTERING")

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
    date_before_to_cleanup = input("Enter a date before all jobs should be deleted (example '2020-12-31 23:59:59')\n")
    validateDate(date_before_to_cleanup)
    return date_before_to_cleanup


def findJobsForCleanUp(session, primecast_account_id, date_before_to_cleanup):
    job_rows = session.execute(
        f"SELECT id,name FROM mep_job WHERE enddate < '{date_before_to_cleanup}' AND  account = {primecast_account_id} \
         ALLOW FILTERING")
    job_ids_to_delete = []
    for job_row in job_rows:
        print(f"Candidate for Deletion: Job name => {job_row.name}")
        job_ids_to_delete.append(job_row.id)
    return job_ids_to_delete


def getIdFromPrimecastAccountNameIdMap(primecast_account_name_id_map):
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
            print('Invalid Input')
            return confirm_deletion_of_jobs()
    except Exception as error:
        print("Please enter valid inputs")
        print(error)
        return confirm_deletion_of_jobs()


def deleteTemplates(session, template_ids):
    for template_id in template_ids:
        session.execute(f"DELETE FROM mep_template WHERE id = {template_id}")


def deleteJobTemplates(session, job_dict):
    print(f"Deleting templates for job: {job_dict['name']}")
    job_id = job_dict["id"]
    template_ids = []
    template_rows = session.execute(f"SELECT id FROM mep_template WHERE job = {job_id} ALLOW FILTERING")
    for template_row in template_rows:
        template_ids.append(template_row.id)
    if len(template_ids) > 0:
        print(f"Found {len(template_ids)} template(s)")
        deleteTemplates(session, template_ids)
        # time.sleep(1)
    else:
        print(f"Found {len(template_ids)}  templates to delete")


def deleteNotifications(session, job_notification_ids):
    for job_notification_id in job_notification_ids:
        session.execute(f"DELETE FROM mep_notification WHERE id = {job_notification_id}")


def deleteJobNotifications(session, job_dict):
    print(f"Deleting notifications for job: {job_dict['name']}")
    job_id = job_dict["id"]
    job_notification_ids = []
    notification_rows = session.execute(f"SELECT id FROM mep_notification WHERE flight = {job_id} ALLOW FILTERING")
    for notification_row in notification_rows:
        job_notification_ids.append(notification_row.id)

    if len(job_notification_ids) > 0:
        print(f"Found {len(job_notification_ids)} Notifications(s)")
        deleteNotifications(session, job_notification_ids)
        # time.sleep(1)
    else:
        print(f"Found {len(job_notification_ids)}  Notifications to delete")


def findJobById(session, job_id):
    row = session.execute(f"SELECT id, name, parameterisedcontactlist, location, profile FROM mep_job WHERE id = {job_id}").one()
    job_dict = {"id": row.id, "name": row.name, "parameterisedcontactlist": row.parameterisedcontactlist,
                "location": row.location, "profile": row.profile}
    return job_dict


def deleteParameterisedListJob(session, parameterised_list_id):
    session.execute(f"DELETE FROM mep_parameterisedlist_job WHERE parameterisedlist= {parameterised_list_id}")


def deleteParameterisedList(session, parameterised_list_id):
    session.execute(f"DELETE FROM mep_parameterisedlist WHERE id = {parameterised_list_id}")


def deleteParameterisedIntermediate(session, parameterised_list_id):
    paramaterised_list_item_ids = []
    parameterisedlist_msisdn_rows = session.execute(f"SELECT parameterisedlistitem FROM mep_parameterisedlist_msisdn WHERE parameterisedlist = {parameterised_list_id}")
    for parameterisedlist_msisdn_row in parameterisedlist_msisdn_rows:
        paramaterised_list_item_ids.append(parameterisedlist_msisdn_row.parameterisedlistitem)

    if len(paramaterised_list_item_ids) > 0:
        for paramaterised_list_item_id in paramaterised_list_item_ids:
            session.execute(f"DELETE FROM mep_parameterisedlistitem WHERE id = {paramaterised_list_item_id}")
        session.execute(f"DELETE FROM mep_parameterisedlist_msisdn WHERE parameterisedlist = {parameterised_list_id}")


def deleteJobParameterisedList(session, job_dict):
    parameterised_list_id = job_dict["parameterisedcontactlist"]
    if bool(parameterised_list_id):
        print(f"Deleting parameterised list for job: {job_dict['name']}")
        deleteParameterisedIntermediate(session, parameterised_list_id)
        deleteParameterisedList(session, parameterised_list_id)
        deleteParameterisedListJob(session, parameterised_list_id)
    else:
        print(f"Found no parameterised list for job: {job_dict['name']}")


def deleteFlightStatusHistory(session, job_dict):
    print(f"Deleting flightstatushistory for job: {job_dict['name']}")
    job_id = job_dict["id"]
    flightstatushistory_ids = []
    flightstatushistory_rows = session.execute(f"SELECT id FROM flightstatushistory WHERE flight = {job_id} ALLOW FILTERING")
    for flightstatushistory_row in flightstatushistory_rows:
        flightstatushistory_ids.append(flightstatushistory_row.id)

    if len(flightstatushistory_ids) > 0:
        print(f"Found {len(flightstatushistory_ids)} flightstatushistory(s) to delete")
        for flightstatushistory_id in flightstatushistory_ids:
            session.execute(f"DELETE FROM flightstatushistory WHERE id = {flightstatushistory_id}")
    else:
        print(f"Found {len(flightstatushistory_ids)}  flight status history to delete")


def deleteJob(session, job_dict):
    job_id = job_dict["id"]
    print(f"Deleting job name={job_dict['name']} id={job_dict['id']}")
    session.execute(f"DELETE FROM mep_job WHERE id = {job_id}")


def scheduleJobsForDelete(session, job_ids_to_delete):
    confirmation = confirm_deletion_of_jobs()
    if confirmation:
        for job_id in job_ids_to_delete:
            job_dict = findJobById(session, job_id)
            print(f"Executing cleanup for job: {job_dict}")
            print("--------------------------------------")
            deleteJobTemplates(session, job_dict)
            deleteJobNotifications(session, job_dict)
            deleteJobParameterisedList(session, job_dict)
            deleteFlightStatusHistory(session, job_dict)
            deleteJob(session, job_dict)
            time.sleep(2)
            print("--------------------------------------")


def cleanUpJobs():
    session = initializeCassandraSession()
    primecast_account_name_id_map = loadPrimeCastAccount(session)
    date_before_to_cleanup = userInputDate()
    primecast_account_id = getIdFromPrimecastAccountNameIdMap(primecast_account_name_id_map)
    job_ids_to_delete = findJobsForCleanUp(session, primecast_account_id, date_before_to_cleanup)
    scheduleJobsForDelete(session, job_ids_to_delete)


cleanUpJobs()
