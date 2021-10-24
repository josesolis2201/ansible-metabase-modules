# Metabase Modules
An Ansible Collection providing modules for interfacing with Metabase.

Contains the following modules:

- `manage_postgresql_db` for registering, updataing or removing postgresql databases into the metabase UI

## Example Usage

### Importing The Collection

`ansible-galaxy collection install josesolis2201.metabase`

### Using the Modules

#### manage_postgresql_db

```yaml
- hosts: localhost
  tasks:

    - name: test module
      manage_postgresql_db:
        metabase_url: metabase.example.com # required
        metabase_scheme: https # default = https
        metabase_api_token: "{{ metabase_api_token }}" # required
        psql_host: psql.example.com # required
        psql_port: "5432" # default = 5432
        psql_user: metabase # required
        psql_password: "{{ metabase_password }}" # required
        database: TestDB # required
        database_name_override: stg-testdb # default = null
        state: present # default = present
```