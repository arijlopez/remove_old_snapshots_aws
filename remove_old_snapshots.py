#!/usr/bin/python
### BEGIN remove_old_snapshots.py INFO
# Remove  snapshots from aws depending on the conditions. It could be easily adapted
# This script assumes that the server where running has .aws/config set up to connect to aws
# Cron to run the script
# 0 13 * * 4 <path-to-script>/remove_old_snapshots.py 2>&1 >> /var/log/remove_old_snapshots.log
### END remove_old_snapshots.py INFO
# Author: Ari Lopez

import boto3
import datetime
import time
from collections import Counter
import operator
import re
import sys

days7 = 604800  # 604800 seconds = 7 days
days30 = 2592000  # 2592000 seconds = 30 days
days60 = 5184000 # 5184000 seconds = 60 days
days90 = 7776000  # 7776000 seconds = 90 days

separator="*********************************************************************"

days_of_week = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday'
}

# converts seconds to days for readability
def sec_to_days(seconds):
    if seconds == 604800:
        return '7'
    elif seconds == 2592000:
        return '30'
    elif seconds == 5184000:
        return '60'
    elif seconds == 7776000:
        return '90'
    else:
        return 'Not sure about the number of days'

# connect to amazon ec2
client = boto3.client('ec2')

# returns present date
def today():
    now = time.strftime("%c")
    return str(now)

def day(snapshot):
    now = datetime.datetime.today()
    day = now.replace(tzinfo=None) - snapshot['StartTime'].replace(tzinfo=None)
    return day

# gets all the snapshots
def get_snapshots():
    counter = 1
    snapshots = 0
    while True:
        try:
            print "%s: Connecting to EC2 to retrieve snapshots with attempt: %s" % (today(),counter)
            if counter >= 21:
                print "%s: Tried to connect to EC2 %s times, there must be a problem with AWS" % (today(),counter)
                exit()
            counter += 1
            snapshots = client.describe_snapshots()
        except Exception as ex:
            print "%s: Something went wrong: %s" % (today(),ex)
            continue
        break
    return snapshots['Snapshots']

# gets the old snapshots
def get_old_snapshots(snapshots):
    old_snapshots = []
    for snapshot in snapshots:
        for key, values in snapshot.iteritems():
            if "Description" in key and "AST_PROD_DBSS" in values:
                old_snapshots.append(snapshot)
    if len(old_snapshots) > 0:
        return old_snapshots
    else:
        print "%s: There are no snapshots to remove, exiting script" % today()
        sys.exit()

# function to findout the closest old set to the start of the month
def keep_set_of_old_snapshots(dates_of_sets):
    dates = Counter(dates_of_sets)
    value8=0
    max_value=0
    for key, value in dates.iteritems():
        if value == 8:
            date=key
            value8=value
            break
    if value8 != 8:
        max_value = max(dates.iteritems(), key=operator.itemgetter(1))[1]
        for key, value in dates.iteritems():
            if value == max_value:
                date=key
                break
        return date
    else:
        return date

# finds sunday snapshots between a range
def get_sunday_old_sets_of_snapshots_between_range(snapshots, min_days, max_days):
    old_snapshots = []
    dates = []
    counter = 0
    regexp = re.compile('AST_PROD_DBSS.*-DATA')
    for snapshot in snapshots:
        if day(snapshot).total_seconds() > min_days and day(snapshot).total_seconds() < max_days:
            if days_of_week[snapshot['StartTime'].weekday()] == 'Sunday' and regexp.match(snapshot['Description']):
                old_snapshots.append(snapshot)
                dates.append(snapshot['StartTime'].date().strftime('%Y-%m-%d'))
                counter += 1
    if len(dates) == 0:
        print "%s: There were no snapshots to remove in %s to %s days old range" % (today(),sec_to_days(min_days),sec_to_days(max_days))
        print separator
    else:
        date = keep_set_of_old_snapshots(dates)
        sunday_snapshots = sorted(old_snapshots, key=lambda k: k['StartTime'])
        if len(sunday_snapshots) == 0:
            print "%s: There are no Sunday snapshots to remove between %s and %s days old range" % (today(),sec_to_days(min_days),sec_to_days(max_days))
        elif len(sunday_snapshots) == 1:
            print "%s: There is only one Sunday snapshot set between %s and %s days old range, nothing to remove" % (today(),sec_to_days(min_days),sec_to_days(max_days))
        else:
            counter = 0
            print "%s: Start function that removes set of Sunday data snapshots with range between %s and %s days old, and leaves closer set to the start of the month" % (today(),sec_to_days(min_days),sec_to_days(max_days))
            for sunday_snapshot in sunday_snapshots:
                if sunday_snapshot['StartTime'].date().strftime('%Y-%m-%d') != date:
                    #print "%s: Snapshot deleted with ID: %s, Description: %s, Date: %s and day of the week: %s, if between %s and %s days old" % (today(),sunday_snapshot['SnapshotId'], sunday_snapshot['Description'], sunday_snapshot['StartTime'].date().strftime('%Y-%m-%d'), days_of_week[sunday_snapshot['StartTime'].weekday()],sec_to_days(min_days),sec_to_days(max_days))
                    remove_snapshot(sunday_snapshot)
                    counter += 1
            print "%s: %s Sunday data old snapshots removed between %s and %s days old, if not removal errors. Left the set closest to the start of the month " % (today(), counter, sec_to_days(min_days),sec_to_days(max_days))
            print separator

# finds all LOG snapshots older than 7 days
def log_snapshot_older_than_7_days(snapshots):
    counter = 0
    regexp = re.compile('AST_PROD_DBSS.*-LOG')
    print "%s: Start function that removes old LOG snapshots older than 7 days " % today()
    for snapshot in snapshots:
        if day(snapshot).total_seconds() > days7:
            if regexp.match(snapshot['Description']):
                #print "%s: Snapshot deleted with ID: %s, Description: %s, Date: %s and day of the week: %s, older than 7 days" % (today(),snapshot['SnapshotId'], snapshot['Description'], snapshot['StartTime'].date(), days_of_week[snapshot['StartTime'].weekday()])
                remove_snapshot(snapshot)
                counter += 1
    print "%s: %s old LOG snapshots removed older than 7 days, if not removal errors" % (today(),counter)
    print separator

# finds all the DATA snapshots older than 90 days
def snapshot_older_than_90_days(snapshots):
    counter = 0
    regexp = re.compile('AST_PROD_DBSS.*-DATA')
    print "%s: Start function that removes snapshots older than 90 days " % today()
    for snapshot in snapshots:
        if day(snapshot).total_seconds() > days90:
            if regexp.match(snapshot['Description']):
                #print "%s: Snapshot deleted with ID: %s, Description: %s, Date: %s and day of the week: %s, older than 90 days" % (today(),snapshot['SnapshotId'], snapshot['Description'], snapshot['StartTime'].date().strftime('%Y-%m-%d'), days_of_week[snapshot['StartTime'].weekday()])
                remove_snapshot(snapshot)
                counter += 1
    print "%s: %s old DATA snapshots removed older than 90 days, if not removal errors" % (today(),counter)
    print separator

# finds all the DATA snapshots between 7 and 30 days old except for taken on Sunday
def snapshot_is_not_sunday_between_7_to_30_days_old(snapshots):
    counter = 0
    regexp = re.compile('AST_PROD_DBSS.*-DATA')
    print "%s: Start function that removes DATA old snapshots between 7 and 30 days old" % today()
    for snapshot in snapshots:
        if day(snapshot).total_seconds() > days7 and day(snapshot).total_seconds() < days30:
            if days_of_week[snapshot['StartTime'].weekday()] != 'Sunday' and regexp.match(snapshot['Description']):
                #print "%s: Snapshot deleted with ID: %s, Description: %s, Date: %s and day of the week: %s, between 7 and 30 days old" % (today(),snapshot['SnapshotId'], snapshot['Description'], snapshot['StartTime'].date().strftime('%Y-%m-%d'), days_of_week[snapshot['StartTime'].weekday()])
                remove_snapshot(snapshot)
                counter += 1
    print "%s: %s DATA old snapshots removed between 7 and 30 days except created on Sunday,  if not removal errors" % (today(),counter)
    print separator

# function that removes a snapshot with a snapshot ID
def remove_snapshot(snapshot):
    counter = 0
    while True:
        try:
            if counter >= 3:
                print "%s: Could not remove snapshot after 3 attempts with ID: %s, Description: %s, Date: %s and day of the week: %s, the snapshot may not exist anymore or there could be a problem with AWS" % (today(),snapshot['SnapshotId'], snapshot['Description'], snapshot['StartTime'].date().strftime('%Y-%m-%d'), days_of_week[snapshot['StartTime'].weekday()])
                break
            counter += 1
            snap = client.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
            if snap['ResponseMetadata']['HTTPStatusCode'] == 200:
                print "%s: Snapshot deleted with ID: %s, Description: %s, Date: %s and day of the week: %s" % (today(),snapshot['SnapshotId'], snapshot['Description'], snapshot['StartTime'].date().strftime('%Y-%m-%d'), days_of_week[snapshot['StartTime'].weekday()])
                break
            else:
                print "%s: The snapshot with id: %s and description: %s could not be removed with attempt: %s. Maybe the snapshot has already been removed" % (today(),snapshot['SnapshotId'], snapshot['Description'], counter)
        except Exception as ex:
            print "%s: Something went wrong: %s" % (today(),ex)
            continue
        break

# function for testing purposes
def test_function(snapshots,string):
    counter = 0
    print "%s: Start test function" % today()
    for snapshot in snapshots:
        if  string in snapshot['Description']:
            #print "%s: Snapshot deleted with ID: %s, Description: %s, Date: %s and day of the week: %s, older than 90 days" % (today(),snapshot['SnapshotId'], snapshot['Description'], snapshot['StartTime'].date().strftime('%Y-%m-%d'), days_of_week[snapshot['StartTime'].weekday()])
            # remove_snapshot(snapshot)
            counter += 1
    print "%s: %s snapshots removed in test function" % (today(),counter)

print "%s: Beginning of snapshot removal" % today()
# remove old LOG snapshots older than 7 days
log_snapshot_older_than_7_days(get_old_snapshots(get_snapshots()))
# remove all data old snapshots between 7 and 30 days old except for the ones taken on Sunday
snapshot_is_not_sunday_between_7_to_30_days_old(get_old_snapshots(get_snapshots()))
# remove Sunday set of data old snapshots between 30 to 60 days old except for the closest set to the start of the month
get_sunday_old_sets_of_snapshots_between_range(get_old_snapshots(get_snapshots()),days30,days60)
# remove Sunday set of data old snapshots between 60 to 90 days old except for the closest set to the start of the month
get_sunday_old_sets_of_snapshots_between_range(get_old_snapshots(get_snapshots()),days60,days90)
# remove old old data snapshots older than 90 days
snapshot_older_than_90_days(get_old_snapshots(get_snapshots()))
print "%s: End of snapshot removal" % today()
print separator
