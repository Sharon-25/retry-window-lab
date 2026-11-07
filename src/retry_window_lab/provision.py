"""Bootstrap-only role provisioning. Runtime services never use bootstrap credentials."""
import os
import secrets

from psycopg import sql

from retry_window_lab.config import PROJECT_ROOT, postgres_conninfo
from retry_window_lab.db import connect, table, validate_schema
from retry_window_lab.migrate import migrate
from retry_window_lab.provider_migrate import migrate as provider_migrate


def configure_roles(conninfo, app_password, provider_password,
                    app_role='rwl_app', provider_role='rwl_provider_service',
                    app_schema='rwl', provider_schema='rwl_provider'):
    for name in (app_role, provider_role, app_schema, provider_schema):
        validate_schema(name)
    if app_role == provider_role or app_schema == provider_schema:
        raise ValueError('Separate roles and schemas are required')
    for password in (app_password, provider_password):
        if not isinstance(password, str) or len(password) < 24 or '\x00' in password:
            raise ValueError('Use passwords of at least 24 characters')
    with connect(conninfo) as c, c.transaction():
        c.execute("SELECT pg_advisory_xact_lock(hashtextextended('rwl:provision',0))")
        database = c.execute('SELECT current_database() AS name,current_user AS owner').fetchone()
        for role, password in ((app_role, app_password), (provider_role, provider_password)):
            if role == database['owner']:
                raise ValueError('Runtime role must differ from bootstrap role')
            row = c.execute('SELECT * FROM pg_roles WHERE rolname=%s', (role,)).fetchone()
            if row:
                elevated = any(row[key] for key in ('rolsuper','rolcreatedb','rolcreaterole','rolreplication','rolbypassrls'))
                member = c.execute('SELECT 1 FROM pg_auth_members WHERE member=%s LIMIT 1', (row['oid'],)).fetchone()
                owns = c.execute('SELECT 1 FROM pg_namespace WHERE nspowner=%s UNION ALL SELECT 1 FROM pg_database WHERE datdba=%s UNION ALL SELECT 1 FROM pg_class WHERE relowner=%s LIMIT 1',
                                 (row['oid'],row['oid'],row['oid'])).fetchone()
                if elevated or member or owns:
                    raise ValueError('Refusing to adopt a privileged or owning runtime role')
            else:
                c.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('ALTER ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}')
                      .format(sql.Identifier(role), sql.Literal(password)))
            c.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}')
                      .format(sql.Identifier(database['name']), sql.Identifier(role)))
        for schema in (app_schema, provider_schema):
            c.execute(sql.SQL('REVOKE ALL ON SCHEMA {} FROM PUBLIC,{},{}')
                      .format(sql.Identifier(schema), sql.Identifier(app_role), sql.Identifier(provider_role)))
            c.execute(sql.SQL('REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM PUBLIC,{},{}')
                      .format(sql.Identifier(schema), sql.Identifier(app_role), sql.Identifier(provider_role)))
        grant_application_tables(c, app_schema, app_role)
        c.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(provider_schema), sql.Identifier(provider_role)
        ))
        for name in ('experiments', 'inventory', 'reservations', 'protection_keys', 'requests'):
            c.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE ON {} TO {}").format(
                table(provider_schema, name), sql.Identifier(provider_role)
            ))


def grant_application_tables(connection, schema, role):
    connection.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
        sql.Identifier(schema), sql.Identifier(role)
    ))
    for name in ('tasks', 'attempts', 'outbox', 'provider_context',
                 'provider_dispatches', 'provider_observations', 'recovery_decisions'):
        connection.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE ON {} TO {}").format(
            table(schema, name), sql.Identifier(role)
        ))
    connection.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(
        table(schema, "queue_settings"), sql.Identifier(role)
    ))


def main():
    conninfo = postgres_conninfo()
    # Role names are fixed for the local installation, never inferred from PGUSER.
    values = {'RWL_APP_USER': 'rwl_app', 'RWL_PROVIDER_USER': 'rwl_provider_service'}
    for name in ('RWL_APP_PASSWORD','RWL_PROVIDER_PASSWORD'):
        values[name] = os.environ.get(name) or secrets.token_urlsafe(32)
    path = PROJECT_ROOT / '.env'
    text = path.read_text() if path.exists() else ''
    # Existing explicitly configured role names must agree with the fixed contract.
    for key in ('RWL_APP_USER','RWL_PROVIDER_USER'):
        if os.environ.get(key, values[key]) != values[key]:
            raise SystemExit(f'{key} must match the documented runtime role')
    additions = {key:value for key,value in values.items() if not os.environ.get(key)}
    if additions:
        with path.open('a') as stream:
            stream.write('\n' + '\n'.join(f'{key}={value}' for key,value in additions.items()) + '\n')
        path.chmod(0o600)
        os.environ.update(additions)
    migrate(conninfo)
    provider_migrate(conninfo)
    configure_roles(conninfo,values['RWL_APP_PASSWORD'],values['RWL_PROVIDER_PASSWORD'])
    print('Provider schema and restricted runtime roles configured. No passwords printed.')


if __name__ == '__main__':
    main()
