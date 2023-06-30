from winreg import (
    ConnectRegistry,
    HKEY_LOCAL_MACHINE,
    OpenKeyEx,
    QueryValueEx,
)

def odbc_dsn_2_psyopg2dsn(odbc_dsn, user, pwd ):
    """
    This function converts a windows ODBC DSN to a dsn 
    that can be used by psycopg2.

    It reads connection parameters from the windows registry,
      username and password are often not in there,
      and can be supllied as parameters. 

    """
    # get hostname, port, sslmode from odbc registry 
    hreg = ConnectRegistry(None,HKEY_LOCAL_MACHINE)
    key  = f"SOFTWARE\\ODBC\\ODBC.INI\\{odbc_dsn}"
    hkey = OpenKeyEx(hreg, key)
    hostname   = QueryValueEx(hkey, "Servername")[0]
    portnumber = QueryValueEx(hkey, "Port")[0]
    sslmode    = QueryValueEx(hkey, "SSLmode")[0]
    database   = QueryValueEx(hkey, "Database")[0]
    #set defaults for portnmumber and SSL mode
    if not portnumber:
        portnumber = "5432"
    if not sslmode:
        sslmode = 'Prefer'
        
    # using all of the above, create connection
    return f"dbname={database} user={user} password={pwd} host={hostname} port={portnumber} sslmode={sslmode}"