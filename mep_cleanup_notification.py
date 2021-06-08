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
notification_count_stmt_by_date = None
notification_ids_stmt_by_date = None
notification_delete_by_id = None


def initializePreparedStatements():
    global notification_count_stmt_by_date
    global notification_ids_stmt_by_date
    global notification_delete_by_id

    notification_count_stmt_by_date = session.prepare("SELECT count(*) FROM  mep_notification WHERE created <= ? ALLOW FILTERING")
    notification_ids_stmt_by_date = session.prepare("SELECT id FROM  mep_notification WHERE created <= ? ALLOW FILTERING")
    notification_delete_by_id = session.prepare("DELETE FROM  mep_notification WHERE id = ?")


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


def validateDate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM:SS")


def userInputDate():
    date_before_to_cleanup = input("Enter a date before all Notifications should be deleted (the format should be "
                                   "YYYY-MM-DD HH:MM:SS example '2020-12-31 23:59:59')\n")
    validateDate(date_before_to_cleanup)
    return date_before_to_cleanup


def confirm_deletion_of_notifications():
    check = str(input("Do you want to proceed cleanup of jobs ? (Y/N): ")).lower().strip()
    try:
        if check[0] == 'y':
            return True
        elif check[0] == 'n':
            return False
        else:
            print('Invalid Input.')
            return confirm_deletion_of_notifications()
    except Exception as error:
        print("Please enter valid inputs.")
        print(error)
        return confirm_deletion_of_notifications()


def findTotalNotificationForDelete(date_before_to_cleanup):
    total_notification_row = session.execute(notification_count_stmt_by_date, [datetime.strptime(date_before_to_cleanup, '%Y-%m-%d %H:%M:%S')]).one()
    print(f"Total notifications to be deleted {total_notification_row.count}")
    return total_notification_row.count


def deleteNotifications(date_before_to_cleanup):
    notification_id_rows = session.execute(notification_ids_stmt_by_date, [datetime.strptime(date_before_to_cleanup, '%Y-%m-%d %H:%M:%S')] )
    futures = []
    print("Deleting notification in progress. Please wait")
    for notification_id_row in notification_id_rows:
        future = session.execute_async(notification_delete_by_id, [notification_id_row.id])
        futures.append(future)

    for future in futures:
        future.result()
    print("Successfully deleted notifications")


def scheduleNotificationsCleanUp(date_before_to_cleanup):
    confirmation = confirm_deletion_of_notifications()
    if confirmation:
        total_notification_to_delete = findTotalNotificationForDelete(date_before_to_cleanup)
        if total_notification_to_delete > 0:
            deleteNotifications(date_before_to_cleanup)



def cleanUpNotifications():
    global session
    session = initializeCassandraSession()
    date_before_to_cleanup = userInputDate()
    scheduleNotificationsCleanUp(date_before_to_cleanup)


cleanUpNotifications()


