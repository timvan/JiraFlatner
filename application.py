import requests
import json
import dateutil.parser
import csv
from datetime import timedelta, datetime, timezone
import pickle
import time


# ------------ API REQUESTS ------------ #

def get_issues():

    url = 'https://fullprofile.atlassian.net/rest/api/2/search'

    maxResults = 100
    startAt = 0

    querystring = {
        "jql": "project = ADS order by created ASC",
        # "jql": "project = ADS AND type in standardIssueTypes()",
        "maxResults": maxResults,
        "startAt": startAt,
        # "fields": "status, subtasks, issuetype, summary, aggregatetimespent, aggregatetimeoriginalestimate, aggregatetimeestimate, customfield_10016, assignee, created, timespent, timeoriginalestimate, timeestimate, resolutiondate, fixVersions",
        "fields" : "",
        "expand": "changelog, renderedFields",
    }

    headers = {
        'Authorization': "Basic dGltLnZhbi5lbGxlbWVldDpBZ3JpZGlnaXRhbDEhamlyYQ==",
        'Cache-Control': "no-cache",    
    }

    print("-" * 10)
    print("Getting issues..")
    print("Requesting jql query: ", querystring['jql'])
    print("Filtering for fields: ", querystring['fields'])

    response = requests.request("GET", url, headers=headers, params=querystring)
    parsed = response.json()

    print("Recieved", len(parsed['issues']), "issues starting at", parsed['startAt'], "from a total of", parsed['total'])


    # Check to see if all issues were received, if not repeat request to retrieve all issues and append to original parsed response
    # maxResults is the number of issues received per request. total is the total number of issues within the query on Jira.
    if PAGINATE:
        if  (startAt + maxResults) < parsed['total']:

            remaining_calls = ((parsed['total'] - (startAt + maxResults)) // maxResults) + 1

            for i in range(1, remaining_calls + 1):
                querystring['startAt'] = startAt + i * maxResults
                response = requests.request("GET", url, headers=headers, params=querystring).json()
                print("Recieved", len(response['issues']), "issues starting at", response['startAt'], "from a total of", response['total'])
                for issue in response['issues']:
                    parsed['issues'].append(issue)

    print("Received", len(parsed['issues']), "issues in total")
    return parsed['issues']

def get_all_sprints():

    url = 'https://fullprofile.atlassian.net/rest/agile/1.0/board/12/sprint/'
    headers = {
        'Authorization': "Basic dGltLnZhbi5lbGxlbWVldDpBZ3JpZGlnaXRhbDEhamlyYQ==",
        'Cache-Control': "no-cache",
    }

    querystring = {
        'startAt': 0,
    }

    print("-" * 10)
    print("Getting sprints meta")
    # print("Requesting jql query: ", querystring['jql'])
    # print("Filtering for fields: ", querystring['fields'])

    response = requests.request("GET", url, headers=headers, params=querystring)
    parsed = response.json()
    sprints = parsed['values']
    print("Recieved", len(sprints), 'sprints', 'starting at', querystring['startAt'])

    i = 1
    while not parsed['isLast']:

        querystring['startAt'] = i * parsed['maxResults']
        response = requests.request("GET", url, headers=headers, params=querystring).json()
        print("Recieved", len(response['values']), 'sprints', 'starting at', querystring['startAt'])

        sprints.extend(response['values'])
        parsed['isLast'] = response['isLast']
        i += 1

    print("Received", len(sprints), "sprints in total")

    # sprints.sort(key = lambda e: e['name'], reverse = True)

    return sprints

# ------------ ISSUES ------------ #

def format_issue(i):


    issue = {}


    # issue['aggregateprogress'] = i['fields']['aggregateprogress']

    issue['id'] = i['id']
    issue['self'] = "https://fullprofile.atlassian.net/browse/" + i['key']
    issue['key'] = i['key']

    issue['aggregatetimeestimate'] = i['fields']['aggregatetimeestimate']
    issue['aggregatetimeoriginalestimate'] = i['fields']['aggregatetimeoriginalestimate']
    issue['aggregatetimespent'] = i['fields']['aggregatetimespent']
    issue['assignee'] = i['fields']['assignee']['displayName'] if i['fields']['assignee'] else None
    # issue['components'] = i['fields']['components'] # MANY TO MANY RELATIONSHIP
    issue['created'] = adjust_for_utc(dateutil.parser.parse(i['fields']['created'])).strftime("%d-%m-%Y %H:%M:%S")
    issue['created_week'] = adjust_for_utc(dateutil.parser.parse(i['fields']['created'])).strftime("%Y_%W")
    issue['creator'] = i['fields']['creator']['displayName']
    # issue['sprints'] = i['fields']['customfield_10016'] # MANY TO MANY TABLE REQD
    issue['rootcause'] = i['fields']['customfield_11222']['value'] if i['fields']['customfield_11222'] else None
    # issue['impactedCustomers'] = i['fields']['customfield_11223'] # MANY TO MANY TABLE REQD
    # issue['description'] = i['fields']['description'] # NOT NEEDED AT THIS STAGE
    # issue['fixVersions'] =i['fields']['fixVersions'] # MANY TO MANY TABLE REQD
    # issue['fixVersionsName'] =i['fields']['fixVersions']['name']
    # issue['fixVersionsDescription'] =i['fields']['fixVersions']['description']
    # issue['fixVersionsReleaseDate'] =i['fields']['fixVersions']['releaseDate']
    issue['issuetype'] = i['fields']['issuetype']['name']
    issue['subtask'] = i['fields']['issuetype']['subtask']
    # issue['labels'] = i['fields']['labels'] # LIST TABLE MAYBE REQD
    issue['parentId'] = i['fields']['parent']['id'] if 'parent' in i['fields'] else None
    issue['parentKey'] = i['fields']['parent']['key'] if 'parent' in i['fields'] else None
    issue['priorityId'] = i['fields']['priority']['id']
    issue['priorityName'] = i['fields']['priority']['name']
    issue['projectKey'] = i['fields']['project']['key']
    issue['projectName'] = i['fields']['project']['name']
    issue['reporter'] = i['fields']['reporter']['displayName'] if i['fields']['reporter'] else None
    issue['resolution'] = i['fields']['resolution']['name'] if i['fields']['resolution'] else None
    issue['resolutiondate'] = adjust_for_utc(dateutil.parser.parse(i['fields']['resolutiondate'])).strftime("%d-%m-%Y %H:%M:%S") if i['fields']['resolutiondate'] else None
    issue['resolutiondate_week'] = adjust_for_utc(dateutil.parser.parse(i['fields']['resolutiondate'])).strftime("%Y_%W") if i['fields']['resolutiondate'] else None
    issue['status'] = i['fields']['status']['name']
    issue['statusId'] = i['fields']['status']['statusCategory']['id']
    issue['summary'] = i['fields']['summary']
    issue['storySubtaskKey'] = i['fields']['parent']['key'] if i['fields']['issuetype']['subtask'] else i['key']
    issue['timeestimate'] = i['fields']['timeestimate']
    issue['timeoriginalestimate'] = i['fields']['timeoriginalestimate']
    issue['timespent'] = i['fields']['timespent']
    issue['updated'] = adjust_for_utc(dateutil.parser.parse(i['fields']['updated'])).strftime("%d-%m-%Y %H:%M:%S") if i['fields']['updated'] else None
    issue['updated_week'] = adjust_for_utc(dateutil.parser.parse(i['fields']['updated'])).strftime("%Y_%W") if i['fields']['updated'] else None
    # issue['versions'] = i['fields']['versions']
    issue['workratio'] = i['fields']['workratio']



    try:
        if i['fields']['summary'].upper().startswith(("BACK", "API", "PERI"), 1):
            issue['devteam'] = "Backend"
        elif i['fields']['summary'].upper().startswith("FRONT", 1):
            issue['devteam'] = "Front End"
        elif i['fields']['summary'].upper().startswith("TEST", 1):
            issue['devteam'] = "Test"
        elif i['fields']['summary'].upper().startswith("OPS", 1):
            issue['devteam'] = "Ops"
        else:
            issue['devteam'] = None
    except:
        issue['devteam'] = None





    return translate_dict('issue_', issue)

def issues_to_csv(issues):

    filename = 'issues_' + datetime.now().strftime("%Y-%m-%d_%H%M") + ".csv"

    with open(filename, 'w') as csvfile:
        
        keys = format_issue(issues[0]).keys()
        writer = csv.DictWriter(csvfile, fieldnames = keys)
        writer.writeheader()

        for i in issues:
            writer.writerow(format_issue(i))

# ------------ ISSUES TO SPRINTS MANY TO MANY ------------ #

def sprints_in_issue(i):

    sprints_raw = i['fields']['customfield_10016']
    
    sprints = []

    if not sprints_raw:
        return None
    
    for s in sprints_raw:

        temp = {}

        item = s.split('[')[1].split(',')[0]
        key = item.split('=')[0]
        value = item.split('=')[1]
        temp[key] = value

        sprints.append({
            'issue_id': i['id'],
            'sprint_id': temp['id'],
            })

    return sprints

def issues_to_sprints_to_csv(issues):

    filename = 'issues_to_sprints_' + datetime.now().strftime("%Y-%m-%d_%H%M") + ".csv"

    with open(filename, 'w') as csvfile:
        
        fieldnames = ['issue_id', 'sprint_id']

        writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
        writer.writeheader()

        for i in issues:
            sprints = sprints_in_issue(i)

            if sprints:
                for s in sprints:
                    writer.writerow(s)

# ------------ SPRINTS ------------ #

def format_sprint(s):

    sprint = {}

    sprint['id'] = s['id']
    sprint['state'] = s['state']
    sprint['name'] = s['name']
    sprint['startDate'] = adjust_for_utc(dateutil.parser.parse(s['startDate'])).strftime("%d-%m-%Y %H:%M:%S") if 'startDate' in s else None
    sprint['startDate_week'] = adjust_for_utc(dateutil.parser.parse(s['startDate'])).strftime("%Y_%W") if 'startDate' in s else None
    sprint['endDate'] = adjust_for_utc(dateutil.parser.parse(s['endDate'])).strftime("%d-%m-%Y %H:%M:%S") if 'endDate' in s else None
    sprint['endDate_week'] = adjust_for_utc(dateutil.parser.parse(s['endDate'])).strftime("%Y_%W") if 'endDate' in s else None
    sprint['completeDate'] = adjust_for_utc(dateutil.parser.parse(s['completeDate'])).strftime("%d-%m-%Y %H:%M:%S") if 'completeDate' in s else None
    sprint['completeDate_week'] = adjust_for_utc(dateutil.parser.parse(s['completeDate'])).strftime("%Y_%W") if 'completeDate' in s else None
    sprint['goal'] = s['goal']

    return translate_dict('sprint_', sprint)

def sprints_to_csv(sprints):

    filename = 'sprints_' + datetime.now().strftime("%Y-%m-%d_%H%M") + ".csv"

    with open(filename, 'w') as csvfile:
        
        keys = format_sprint(sprints[0]).keys()
        # keys = ['issues_' + k for k in keys]
        writer = csv.DictWriter(csvfile, fieldnames = keys)
        writer.writeheader()

        for s in sprints:
            writer.writerow(format_sprint(s))

# ------------ CHANGELOG ------------ #

def in_which_sprint(issue, event_date):

    sprints = issue['fields']['customfield_10016']

    sprintsFormatted = []

    for s in sprints:

        temp = {}

        for i in s.split('[')[1].split(','):
            if '=' in i:
                key = str(i.split('=')[0])
                value = str(i.split('=')[1])
                temp[key] = value

        # print(temp)
        # print('-'*10)

        if temp['state'] == 'FUTURE':
            continue

        newSprint = {}
        newSprint['id'] = int(temp['id'])
        newSprint['state'] = temp['state']

        try:
            newSprint['startDate'] = dateutil.parser.parse(temp['startDate'])
        except:
            print('-' * 10)
            print('ERROR in_which_sprint startDate:', 'issues_id', issue['id'])
            print('temp id:', temp['id'])
            newSprint['startDate'] = None


        try:
            newSprint['endDate'] = dateutil.parser.parse(temp['endDate'])
        except:
            print('-' * 10)
            print('ERROR in_which_sprint endDate:', 'issues_id', issue['id'])
            print('temp id:', temp['id'])
            newSprint['endDate'] = None


        if temp['state'] == 'ACTIVE':
            newSprint['completeDate'] = None
        else:
            try:
                newSprint['completeDate'] = dateutil.parser.parse(temp['completeDate'])
            except:
                print('-' * 10)
                print('ERROR in_which_sprint completeDate:', 'issues_id', issue['id'])
                print('temp id:', temp['id'])
                newSprint['completeDate'] = None

        sprintsFormatted.append(newSprint)

    sprintsFormatted.sort(key = lambda e: e['startDate'], reverse = False)
    # print([e['startDate'] for e in sprintsFormatted])

    if len(sprintsFormatted) == 0:
        return None

    if event_date < sprintsFormatted[0]['startDate']:
        return None
    


    for s in sprintsFormatted:

        if str(issue['id']) == '32924':
            print(s['id'])

        if s['state'] == 'ACTIVE':
            return s['id']

        if event_date < s['completeDate']:
            
            if str(issue['id']) == '32924':
                print(event_date, s['completeDate'])

            return s['id']

def format_changelog(i):
    changelog = i['changelog']
    changelogFormatted = []

    # Sort current changelog in descending order - created first is now top
    histories = changelog['histories'][::-1]

    for i_change, change in enumerate(histories):
        for item in change['items']:

            # Filter changelog for fields
            # if item['field']:
            # if item['field'] in ['timespent', 'timeestimate', 'timeoriginalestimate', 'status', 'WorklogId', 'WorklogTimeSpent', 'resolution', 'resolutiondate', 'Sprint']:
            if item['field'] in ['timespent']:
            # if item['field'] not in ['description', 'Attachment', 'assignee', 'Parent', 'Fix Version', 'summary']:

                # Check current item in change against all items in previous change for duplicates, if duplicate contiune onto the next item not appending a newItem
                if i_change > 0:
                    if (is_item_in_prev_change(item, histories[i_change - 1]['items'])):
                        #print("Duplicate changelog item detected", item['field'])
                        continue

                # store new formatted item in formatted changelog
                newItem = {
                    'issue_id': i['id'],
                    'changelog_id': change['id'],
                    'changelog_created': adjust_for_utc(dateutil.parser.parse(change['created'])).strftime("%d-%m-%Y %H:%M:%S"),
                    'changelog_created_week': adjust_for_utc(dateutil.parser.parse(change['created'])).strftime("%Y_%W"),
                    'changelog_field': item['field'],
                    'changelog_from': item['fromString'],
                    'changelog_to': item['toString'],
                    'sprint_id': in_which_sprint(i, dateutil.parser.parse(change['created'])) if i['fields']['customfield_10016'] else None,
                }

                try:
                    newItem['changelog_author'] = change['author']['displayName']
                except:
                    print('ERROR format_changelog: no display name')

                changelogFormatted.append(newItem)

    #print(json.dumps(changelogFormatted, indent = 4))
    return changelogFormatted

def changelog_to_csv(issues):

    filename = 'issues_changelog_' + datetime.now().strftime("%Y-%m-%d_%H%M") + ".csv"

    with open(filename, 'w') as csvfile:
        
        fieldnames = ['issue_id', 'changelog_id', 'changelog_author', 'changelog_created', 'changelog_created_week', 'changelog_field', 'changelog_from', 'changelog_to', 'sprint_id']

        writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
        writer.writeheader()

        for i in issues:
            changelog = format_changelog(i)

            if changelog:
                for c in changelog:
                    writer.writerow(c)

# ------------ HELPERS ------------ #

def translate_dict(prefix, item):

    new_item = {}

    for k, v in item.items():

        new_k = prefix + k
        new_item[new_k] = v

    return new_item

def write_data_to_file(file_name, data):

    with open(file_name, 'w') as f:
        json.dump(data, f)

def is_item_in_prev_change(item, prev_change):
    """
    Check current item against all items in previous change, return True if current item is a duplicate item else return False
    """
    for prev_item in prev_change:
        #print("checking", item, "\nagainst", prev_item)
        if prev_item['field'] == item['field'] and item['from'] == prev_item['from'] and item['to'] == prev_item['to']:
            return True

    return False

def adjust_for_utc(datetime_obj):

    if str(datetime_obj.tzinfo) == 'tzutc()':
        return datetime_obj + timedelta(hours = 11)

    else:
        return datetime_obj




# ------------ SETUP ------------ #

if __name__ == "__main__":

    PAGINATE = True
    OFFLINE_MODE = True
    
    if OFFLINE_MODE:
        print('-'*20)
        print('!! OFFLINE MODE !!')
        print('-'*20)

        with open('issues.pkl', 'rb') as f:
            issues = pickle.load(f)
        with open('sprints.pkl', 'rb') as f:
            sprints = pickle.load(f)

    else:
        issues = get_issues()
        with open('issues.pkl', 'wb') as f:
            pickle.dump(issues, f)

        sprints = get_all_sprints()
        with open('sprints.pkl', 'wb') as f:
            pickle.dump(sprints, f)

        write_data_to_file('issues.json', issues)
        write_data_to_file('sprints.json', sprints)    



    issues_to_csv(issues)
    issues_to_sprints_to_csv(issues)
    sprints_to_csv(sprints)
    changelog_to_csv(issues)

