from ansible.module_utils.common.validation import safe_eval
from ansible.module_utils.basic import AnsibleModule
import requests
import os
import time
import psycopg2


def getApiToken(base_host, username, password):

    data = {
        "username": username,
        "password": password
    }
    host = base_host + "/api/session"

    response = requests.post(host, json=data)

    if response.status_code == 200:
        api_token = response.json()['id']
        return(api_token, response.status_code)
    else:
        error_message = "Authentication for %s failed"%host
        return(error_message, response.status_code)


def getTargetDatabases(psql_user, psql_password, psql_host, database_filter_list, database):

    unwanted_chars = '(),'

    connection = psycopg2.connect(host=psql_host, port=5432, database=database, user=psql_user, password=psql_password)
    cursor = connection.cursor()
    cursor.execute ("""SELECT datname FROM pg_database""")
    query_results = cursor.fetchall()

    clean_results = [''.join(x for x in string if not x in unwanted_chars) for string in query_results]
    database_list = list(set(clean_results) - set(database_filter_list))

    return(database_list)

def getCurrentDatabases(api_token, base_host):

    headers = { "X-Metabase-Session: %s"}%api_token
    host = base_host + "/api/database"

    response = requests.post(host, headers=headers).json()


    if response.status_code == 200:
        api_token = response.json()['id']
        return(api_token, response.status_code)
    else:
        error_message = "Authentication for %s failed"%host
        return(error_message, response.status_code)

def manageDatabases(data):
    has_changed = False
    is_error = False

    if data['metabase_scheme']:
        metabase_scheme = data['metabase_scheme'] + "://"
    else:
        metabase_scheme = "https://"

    if data['exclude_db_list']:
        database_filter = data['exclude_db_list']
    else:
        database_filter = []

    if data['database']:
        psql_database = data['database']
    else:
        psql_database = "postgres"

    metabase_host = metabase_scheme + data['metabase_url']
    metabase_user = data['metabase_admin_user']
    metabase_password = data['metabase_admin_password']
    psql_user = data['psql_user']
    psql_password = data['psql_password']
    psql_host = data['psql_host']

    temp_token, api_token_status_code = getApiToken(metabase_host,
                                                    metabase_user,
                                                    metabase_password)

    if api_token_status_code != 200:
        is_error = True
        meta = {"error": "%s"}%temp_token
        return (is_error, has_changed, meta)
    else:
        api_token = temp_token

    current_databases = getCurrentDatabases(api_token, metabase_host)

    target_databases = getTargetDatabases(psql_user,
                                          psql_password,
                                          psql_host,
                                          database_filter,
                                          psql_database)



def main():

    fields = {
        "metabase_url": {"required": True, "type": "str"},
        "metabase_scheme": {"required": False, "type": "str"},
        "metabase_admin_user": {"required": True, "type": "str"},
        "metabase_admin_password": {"required": True, "type": "str", "no_log": True},
        "psql_user": {"required": True, "type": "str"},
        "sql_password": {"required": True, "type": "str", "no_log": True},
        "psql_host": {"required": True, "type": "str"},
        "database_prefix": {"required": False, "type": "str"},
        "database_lowercase": {"required": False, "type": "str"},
        "exclude_db_list": {"required": False, "type": "str"}
    }

    module = AnsibleModule(argument_spec=fields)
    is_error, has_changed, result = manageDatabases(module.params)

    if not is_error:
        module.exit_json(changed=has_changed, meta=result)
    else:
        module.fail_json(msg="Something went wrong.", meta=result)

if __name__ == '__main__':
    main()