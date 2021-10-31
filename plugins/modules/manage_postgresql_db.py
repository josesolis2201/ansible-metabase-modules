from ansible.module_utils.common.validation import safe_eval
from ansible.module_utils.basic import AnsibleModule
import requests
from requests.api import delete

def buildSessionId(api_token):
    auth_header = {'X-Metabase-Session': api_token}

    return(auth_header)


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


def getCurrentDatabases(api_token, base_host):

    current_databases = []
    auth_header = buildSessionId(api_token)
    host = base_host + "/api/database/"
    response = requests.get(host, headers=auth_header)

    if response.status_code == 200:
        for db_details in response.json()['data']:
            if db_details['engine'] == "postgres":
                if "db" in db_details['details']:
                    tmp_dict = {"ui_name": db_details['name'],
                                "backend_name": db_details['details']['db'],
                                "id": db_details['id']}
                    current_databases.append(tmp_dict)
                else:
                    tmp_dict = {"ui_name": db_details['name'],
                    "backend_name": db_details['details']['dbname'],
                    "id": db_details['id']}
                    current_databases.append(tmp_dict)
        return(current_databases, response.status_code)
    else:
        return(current_databases, response.status_code)


def updateExistingDB(base_host,
                     psql_user,
                     psql_password,
                     psql_host,
                     psql_port,
                     api_token,
                     database,
                     database_ui_name,
                     id):

    auth_header = buildSessionId(api_token)
    json_data = {
        "engine": "postgres",
        "name": database_ui_name,
        "details": {
            "db": database,
            "dbname": database,
            "host": psql_host,
            "user": psql_user,
            "password": psql_password,
            "port": psql_port,
        }
    }

    host = base_host + "/api/database/" + str(id)

    update_request = requests.put(host, headers=auth_header, json=json_data)
    return(update_request.status_code)


def registerNewDB(base_host,
                  psql_user,
                  psql_password,
                  psql_host,
                  psql_port,
                  api_token,
                  database,
                  database_ui_name):

    auth_header = buildSessionId(api_token)
    json_data = {
        "engine": "postgres",
        "name": database_ui_name,
        "details": {
            "dbname": database,
            "db": database,
            "host": psql_host,
            "user": psql_user,
            "password": psql_password,
            "port": psql_port,
        }
    }

    host = base_host + "/api/database/"

    update_request = requests.post(host, headers=auth_header, json=json_data)
    return(update_request.status_code)

def deleteDB(base_host,
             api_token,
             id):

    auth_header = buildSessionId(api_token)

    host = base_host + "/api/database/" + str(id)

    delete_request = requests.delete(host, headers=auth_header)

    return(delete_request.status_code)


def manageDatabaseAbsent(data):
    has_changed = False
    is_error = False

    if data['database_name_override']:
        database_ui_name = data['database_name_override']
    else:
        database_ui_name = data['database']

    metabase_scheme = data['metabase_scheme'] + "://"

    metabase_host = metabase_scheme + data['metabase_url']
    database = data['database']
    api_token = data['metabase_api_token']

    current_databases, query_validation = getCurrentDatabases(api_token, metabase_host)

    if query_validation != 200:
        is_error = True
        meta = {"error": "Got %s status code when trying to query current database list"%str(query_validation)}
        return (is_error, has_changed, meta)

    for i in current_databases:
        if i['backend_name'] == database:
            id = i['id']
            delete_validation = deleteDB(metabase_host,
                                        api_token,
                                        id)

    if delete_validation == 204:
        has_changed = True
        meta = {"msg": "%s database deleted succesfully"%database_ui_name}
        return (is_error, has_changed, meta)
    elif delete_validation == 200:
        has_changed = False
        meta = {"msg": "%s database is not present in this metabase instance"%database_ui_name}
        return (is_error, has_changed, meta)
    else:
        is_error = True
        meta = {"msg": "Delete operation failed with %s http code"%str(delete_validation)}


def manageDatabasePresent(data):
    has_changed = False
    is_error = False

    if data['database_name_override']:
        database_ui_name = data['database_name_override']
    else:
        database_ui_name = data['database']

    metabase_scheme = data['metabase_scheme'] + "://"

    metabase_host = metabase_scheme + data['metabase_url']
    database = data['database']
    psql_user = data['psql_user']
    psql_password = data['psql_password']
    psql_host = data['psql_host']
    psql_port = data['psql_port']
    api_token = data['metabase_api_token']

    current_databases, query_validation = getCurrentDatabases(api_token, metabase_host)

    if query_validation != 200:
        is_error = True
        meta = {"error": "Got %s status code when trying to query current database list"%str(query_validation)}
        return (is_error, has_changed, meta)

    for i in current_databases:
        if i['ui_name'] == database_ui_name:
            id = i['id']
            manage_validation = updateExistingDB(metabase_host,
                                        psql_user,
                                        psql_password,
                                        psql_host,
                                        psql_port,
                                        api_token,
                                        database,
                                        database_ui_name,
                                        id)
            manage_action = "update"

    if not any(i['ui_name'] == database_ui_name for i in current_databases):
        manage_validation = registerNewDB(metabase_host,
                                    psql_user,
                                    psql_password,
                                    psql_host,
                                    psql_port,
                                    api_token,
                                    database,
                                    database_ui_name)
        manage_action = "register"

    if manage_validation == 200:
        if manage_action == "update":
            has_changed = False
            meta = {"msg": "%s database updated succesfully"%database_ui_name}
            return (is_error, has_changed, meta)
        if manage_action == "register":
            has_changed = True
            meta = {"msg": "%s database created succesfully"%database_ui_name}
            return (is_error, has_changed, meta)
    else:
        if manage_action == "update":
            is_error = True
            meta = {"msg": "Got http code %s when trying to update database"%str(manage_validation)}
            return (is_error, has_changed, meta)
        if manage_action == "register":
            is_error = True
            meta = {"msg": "Got http code %s when trying to register database"%str(manage_validation)}
            return (is_error, has_changed, meta)


def main():

    fields = {
        "metabase_url": {"required": True, "type": "str"},
        "metabase_scheme": {"default": "https", "type": "str"},
        "metabase_api_token": {"required": True, "type": "str", "no_log": True},
        "psql_host": {"required": True, "type": "str"},
        "psql_port": {"default": "5432", "type": "str"},
        "psql_user": {"required": True, "type": "str"},
        "psql_password": {"required": True, "type": "str", "no_log": True},
        "database": {"required": True, "type": "str"},
        "database_name_override": {"required": False, "type": "str"},
        "state": {
            "default": "present",
            "choices": ['present', 'absent'],
            "type": "str"
        }
    }

    choice_map = {
        "present": manageDatabasePresent,
        "absent": manageDatabaseAbsent
    }

    module = AnsibleModule(argument_spec=fields)
    is_error, has_changed, result = choice_map.get(module.params['state'])(module.params)

    if not is_error:
        module.exit_json(changed=has_changed, meta=result)
    else:
        module.fail_json(msg="Something went wrong.", meta=result)


if __name__ == '__main__':
    main()