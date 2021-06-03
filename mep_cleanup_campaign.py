import sys
import time
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
campaign_lookup_stmt_by_enddate_and_account = None
job_count_lookup_stmt_by_campaign_id = None
campaign_lookup_stmt_by_id = None
note_lookup_stmt_by_campaign = None
note_delete_by_id = None
notification_lookup_stmt_by_campaign = None
notification_delete_by_id = None
campaignstatushistory_lookup_by_campaign_id = None
campaignstatushistory_delete_by_id = None
file_lookup_stmt_by_campaign = None
file_delete_by_id = None
campaign_delete_by_id = None

def initializePreparedStatements():
    global primecast_account_lookup_stmt_by_name
    global campaign_lookup_stmt_by_enddate_and_account
    global job_count_lookup_stmt_by_campaign_id
    global campaign_lookup_stmt_by_id
    global note_lookup_stmt_by_campaign
    global note_delete_by_id
    global notification_lookup_stmt_by_campaign
    global notification_delete_by_id
    global campaignstatushistory_lookup_by_campaign_id
    global campaignstatushistory_delete_by_id
    global file_lookup_stmt_by_campaign
    global file_delete_by_id
    global campaign_delete_by_id

    primecast_account_lookup_stmt_by_name = session.prepare("SELECT id FROM mep_primecastaccount where name = ? ALLOW FILTERING")
    campaign_lookup_stmt_by_enddate_and_account = session.prepare("SELECT id,name FROM mep_campaign WHERE enddate < ? AND  account = ? \
         ALLOW FILTERING")
    job_count_lookup_stmt_by_campaign_id = session.prepare("select count(*) from  mep_job where campaign=?")
    campaign_lookup_stmt_by_id = session.prepare("SELECT id, name, createdon, enddate FROM mep_campaign WHERE id = ?")
    note_lookup_stmt_by_campaign = session.prepare("SELECT id FROM  mep_note WHERE campaign = ? ALLOW FILTERING")
    note_delete_by_id = session.prepare("DELETE FROM mep_note WHERE id = ?")
    notification_lookup_stmt_by_campaign = session.prepare("SELECT id FROM mep_notification WHERE campaign = ? ALLOW FILTERING")
    notification_delete_by_id = session.prepare("DELETE FROM mep_notification WHERE id = ?")
    campaignstatushistory_lookup_by_campaign_id = session.prepare("SELECT id FROM campaignstatushistory WHERE campaign = ? ALLOW FILTERING")
    campaignstatushistory_delete_by_id = session.prepare("DELETE FROM campaignstatushistory WHERE id = ?")
    file_lookup_stmt_by_campaign = session.prepare("SELECT id FROM mep_file where campaign = ? ALLOW FILTERING")
    file_delete_by_id = session.prepare("DELETE FROM mep_file WHERE id =?")
    campaign_delete_by_id = session.prepare("DELETE FROM mep_campaign WHERE id =?")


def validateDate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM:SS")


def confirm_deletion_of_campaigns():
    check = str(input("Do you want to proceed cleanup of Campaigns ? (Y/N): ")).lower().strip()
    try:
        if check[0] == 'y':
            return True
        elif check[0] == 'n':
            return False
        else:
            print('Invalid Input.')
            return confirm_deletion_of_campaigns()
    except Exception as error:
        print("Please enter valid inputs.")
        print(error)
        return confirm_deletion_of_campaigns()



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


def userInputDate():
    date_before_to_cleanup = input("Enter a date before all jobs should be deleted(the format should be YYYY-MM-DD HH:MM:SS example '2020-12-31 23:59:59')\n")
    validateDate(date_before_to_cleanup)
    return date_before_to_cleanup


def getIdFromPrimecastAccountNameIdMap(primecast_account_name_id_map):
    primecast_account_id = None
    for i in primecast_account_name_id_map:
        primecast_account_id = primecast_account_name_id_map[i]
        break
    return primecast_account_id


def findCampaignJobs(campaign_id):
    job_count_row = session.execute(job_count_lookup_stmt_by_campaign_id, [campaign_id]);
    return job_count_row.one().count


def findCampaignsForCleanUp(primecast_account_id, date_before_to_cleanup):
    campaign_rows = session.execute(campaign_lookup_stmt_by_enddate_and_account, [datetime.strptime(date_before_to_cleanup, '%Y-%m-%d %H:%M:%S'), primecast_account_id])
    campaign_ids = []
    campaign_id_name_map = {}
    for campaign_row in campaign_rows:
        campaign_ids.append(campaign_row.id)
        campaign_id_name_map[campaign_row.id] = campaign_row.name

    campaign_ids_to_delete = []
    for campaign_id in campaign_ids:
        job_count = findCampaignJobs(campaign_id)
        if job_count < 1:
            campaign_ids_to_delete.append(campaign_id)

    for campaign_id_to_delete in campaign_ids_to_delete:
        print(f"Campaign scheduled for deletion:  {campaign_id_name_map[campaign_id_to_delete]}")

    return campaign_ids_to_delete

def findCampaignById(campaign_id):
    row = session.execute(campaign_lookup_stmt_by_id,[campaign_id]).one()
    campaign_dict = {"id": row.id, "name": row.name, "createdon": str(row.createdon)[:19],
                "enddate": str(row.enddate)[:19] }

    return campaign_dict

def deleteCampaignNotes(campaign_dict):
    print(f"Searching notes for Campaign: {campaign_dict['name']}.")
    campaign_id = campaign_dict["id"]
    note_ids = []
    note_rows = session.execute(note_lookup_stmt_by_campaign, [campaign_id])
    for note_row in note_rows:
        note_ids.append(note_row.id)

    if len(note_ids) > 0:
        print(f"Found {len(note_ids)} Notes to delete.")
        for note_id in note_ids:
            session.execute(note_delete_by_id, [note_id])
    else:
        print(f"Found {len(note_ids)} Notes to delete.")


def deleteNotifications(campaign_notification_ids):
    for campaign_notification_id in campaign_notification_ids:
        session.execute(notification_delete_by_id, [campaign_notification_id])


def deleteCampaignNotifications(campaign_dict):
    campaign_id = campaign_dict["id"]
    campaign_notification_ids = []
    notification_rows = session.execute(notification_lookup_stmt_by_campaign, [campaign_id])

    for notification_row in notification_rows:
        campaign_notification_ids.append(notification_row.id)

    if len(campaign_notification_ids) > 0:
        print(f"Found {len(campaign_notification_ids)} Notifications(s) to delete.")
        deleteNotifications(campaign_notification_ids)
        print(f"Successfully deleted the Notifications.")
    else:
        print(f"Found {len(campaign_notification_ids)} Notifications to delete.")


def deleteCampaignStatusHistory(campaign_dict):
    print(f"Searching campaign status histories for campaign: {campaign_dict['name']}.")
    campaign_id = campaign_dict["id"]
    campaignstatushistory_ids = []
    campaignstatushistory_rows = session.execute(campaignstatushistory_lookup_by_campaign_id, [campaign_id])

    for campaignstatushistory_row in campaignstatushistory_rows:
        campaignstatushistory_ids.append(campaignstatushistory_row.id)

    if len(campaignstatushistory_ids) > 0:
        print(f"Found {len(campaignstatushistory_ids)} campaign status histories to delete.")
        for campaignstatushistory_id in campaignstatushistory_ids:
            session.execute(campaignstatushistory_delete_by_id, [campaignstatushistory_id])
    else:
        print(f"Found {len(campaignstatushistory_ids)} campaign status histories to delete.")


def deleteMediaFiles(file_ids):
    for file_id in file_ids:
        session.execute(file_delete_by_id, [file_id])


def deleteCampaignFiles(campaign_dict):
    print(f"Searching Files for campaign : {campaign_dict['name']}.")
    campaign_id = campaign_dict["id"]
    file_ids = []
    file_rows = session.execute(file_lookup_stmt_by_campaign, [campaign_id])

    for file_row in file_rows:
        file_ids.append(file_row.id)

    if len(file_ids) > 0:
        print(f"Found {len(file_ids)} file(s) to delete.")
        deleteMediaFiles(file_ids)
        print(f"Successfully deleted the file(s).")
    else:
        print(f"Found {len(file_ids)} file(s) to delete.")


def deleteCampaign(campaign_dict):
    campaign_id = campaign_dict["id"]
    print(f"Deleting campaign name={campaign_dict['name']} id={campaign_dict['id']}")
    session.execute(campaign_delete_by_id, [campaign_id])


def deleteCampaignKylo(campaign_dict):
    campaign_id = campaign_dict["id"]
    print(f"Deleting campaign from mep_kylo_campaigns_done name={campaign_dict['name']} id={campaign_dict['id']}")
    result = subprocess.Popen(["kylo_cli","-table", "mep_kylo_campaigns_done", "-ki_del", "-key", str(campaign_id)])
    result.wait()


def scheduleCampaignsForDelete(campaign_ids_to_delete):
    confirmation = confirm_deletion_of_campaigns()
    if confirmation:
        for campaign_id in campaign_ids_to_delete:
            campaign_dict = findCampaignById(campaign_id)
            print(f"Executing cleanup for Campaign: {campaign_dict}.")
            print("--------------------------------------")
            deleteCampaignNotes(campaign_dict)
            deleteCampaignNotifications(campaign_dict)
            deleteCampaignStatusHistory(campaign_dict)
            deleteCampaignFiles(campaign_dict)
            deleteCampaignKylo(campaign_dict)
            deleteCampaign(campaign_dict)
            time.sleep(1)
            print("--------------------------------------")


def cleanUpCampaigns():
    global session
    session = initializeCassandraSession()
    primecast_account_name_id_map = loadPrimeCastAccount()
    date_before_to_cleanup = userInputDate()
    primecast_account_id = getIdFromPrimecastAccountNameIdMap(primecast_account_name_id_map)
    campaign_ids_to_delete = findCampaignsForCleanUp(primecast_account_id, date_before_to_cleanup)

    if len(campaign_ids_to_delete) > 0:
        print(f"Found {len(campaign_ids_to_delete)} to delete")
        scheduleCampaignsForDelete(campaign_ids_to_delete)
    else:
        print("Found no campaigns to delete")


cleanUpCampaigns()
