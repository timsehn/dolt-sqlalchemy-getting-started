from sqlalchemy import (
    create_engine,
    text,
    Table,
    Column,
    Integer,
    String,
    select,
    insert,
    delete,
    ForeignKey,
    MetaData
)

from pprint import pprint


def main():
    engine = create_engine(
	"mysql+mysqlconnector://root@127.0.0.1:3306/sql_alchemy_big_demo"
    )

    # Build our tables
    setup_database(engine)

    print_tables(engine)
            
    # Our first Dolt feature. This will commit the first time
    # But after that nothing has changed so there is nothing to commit.
    dolt_commit(engine, "Tim <tim@dolthub.com>", "Created tables")

    # Examine a Dolt system table: dolt_log
    print_commit_log(engine)

    insert_data(engine)

    print_summary_table(engine)

    print_status(engine)
    print_diff(engine, "employees")

    dolt_commit(engine,
                "Aaron <aaron@dolthub.com>",
                "Inserted data into tables")

    print_commit_log(engine)

    drop_table(engine, "employees_teams")
    print_tables(engine)
    dolt_reset_hard(engine)
    print_tables(engine)

def setup_database(engine):
    metadata_obj = MetaData()

    # This is standard SQLAlchemy
    employees_table = Table(
        "employees",
        metadata_obj,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("last_name", String(255)),
        Column("first_name", String(255))
    )
    
    teams_table = Table(
        "teams",
        metadata_obj,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(255))
    )

    employees_teams_table = Table(
        "employees_teams",
        metadata_obj,
        Column("employee_id",
               ForeignKey("employees.id"),
               primary_key=True,
               autoincrement=False),
        Column("team_id",
               ForeignKey("teams.id"),
               primary_key=True,
               autoincrement=False)
    )

    metadata_obj.create_all(engine)

def load_tables(engine):
    metadata_obj = MetaData()

    employees = Table("employees", metadata_obj, autoload_with=engine)
    teams = Table("teams", metadata_obj, autoload_with=engine)
    employees_teams = Table("employees_teams",
                            metadata_obj,
                            autoload_with=engine)

    return (employees, teams, employees_teams)


def insert_data(engine):
    # Start fresh so we can re-run this script
    (employees, teams, employees_teams) = load_tables(engine)
    
    delete_employees_stmt = delete(employees)
    delete_teams_stmt = delete(teams)
    delete_employees_teams_stmt = delete(employees_teams)
    with engine.connect() as conn:
        conn.execute(delete_employees_teams_stmt)
        conn.execute(delete_employees_stmt)
        conn.execute(delete_teams_stmt)
        conn.commit()

    # This is standard SQLAlchemy
    stmt = insert(employees).values([
        {'id':0, 'last_name':'Sehn', 'first_name':'Tim'}, 
        {'id':1, 'last_name':'Hendriks', 'first_name':'Brian'}, 
        {'id':2, 'last_name':'Son', 'first_name':'Aaron'}, 
        {'id':3, 'last_name':'Fitzgerald', 'first_name':'Brian'}
        ]);
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    stmt = insert(teams).values([
        {'id':0, 'name':'Engineering'},
        {'id':1, 'name':'Sales'}
    ])
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    stmt = insert(employees_teams).values([
        {'employee_id':0, 'team_id':0},
        {'employee_id':1, 'team_id':0},
        {'employee_id':2, 'team_id':0},
        {'employee_id':0, 'team_id':1},
        {'employee_id':3, 'team_id':1},
    ])
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

def drop_table(engine, table):
    (employees, teams, employees_teams) = load_tables(engine)

    if ( table == "employees"):
        employees.drop(engine)
    elif ( table ==  "teams" ):
        teams.drop(engine)
    elif ( table == "employees_teams" ):
        employees_teams.drop(engine)
    else:
        print(table + ": Not found") 
        
def dolt_commit(engine, author, message):
    # Dolt exposes version control writes as procedures
    # Here, we use text to execute procedures.
    #
    # The other option is to do something like:
    #
    # conn = engine.raw_connection()
    # results = conn.cursor().callproc('dolt_commit', arguments)
    # conn.close()
    #
    # I like the text approach better.

    with engine.connect() as conn:
        # -A means all tables
        conn.execute(
            text("CALL DOLT_ADD('-A')")
        )
        # --skip-empty so this does not fail if there is nothing to commit
        result = conn.execute(
            text("CALL DOLT_COMMIT('--skip-empty', '--author', '"
                 + author
                 + "', '-m', '"
                 + message
                 + "')")
        )
        commit = None
        for row in result:
            commit = row[0]
        if ( commit ): 
            print("Created commit: " + commit )

def dolt_reset_hard(engine):
    with engine.connect() as conn:
        results = conn.execute(
            text("CALL DOLT_RESET('--hard')")
        )
        conn.commit()
    
            
def print_commit_log(engine):
    # Examine a dolt system table: dolt_log using reflection
    metadata_obj = MetaData()
    print("Commit Log:")

    dolt_log = Table("dolt_log", metadata_obj, autoload_with=engine)
    stmt = select(dolt_log.c.commit_hash,
                  dolt_log.c.committer,
                  dolt_log.c.message
                  ).order_by(dolt_log.c.date.desc())

    with engine.connect() as conn:
        results = conn.execute(stmt)
        for row in results:
            commit_hash = row[0]
            author      = row[1]
            message     = row[2]
            print("\t" + commit_hash + ": " + message + " by " + author)

def print_status(engine):
    metadata_obj = MetaData()
    dolt_status = Table("dolt_status", metadata_obj, autoload_with=engine)

    print("Status")
    stmt = select(dolt_status.c.table_name, dolt_status.c.status)
    with engine.connect() as conn:
        results = conn.execute(stmt)
        rows = results.fetchall();
        if ( len(rows) > 0 ):
            for row in rows:
                table  = row[0]
                status = row[1]
                print("\t" + table + ": " + status)
        else:
            print("\tNo tables modified")

def print_diff(engine, table):
    metadata_obj = MetaData()

    print("Diffing table: " + table)
    dolt_diff = Table("dolt_diff_" + table,
                      metadata_obj,
                      autoload_with=engine)

    stmt = select(dolt_diff)
    with engine.connect() as conn:
        results = conn.execute(stmt)
        for row in results:
            # I use a dictionary here because dolt_diff_<table> is a wide table
            row_dict = row._asdict()
            # Then I use pprint to display the results
            pprint(row_dict)
    
def print_tables(engine):
    # Raw SQL here to show what we've done
    with engine.connect() as conn:
        result = conn.execute(text("show tables"))

        print("Tables in database:")
        for row in result:
            table = row[0]
            print("\t" + table)
            
def print_summary_table(engine):
    (employees, teams, employees_teams) = load_tables(engine)

    print("Team Summary")

    # Dolt supports up to 12 table joins. Here we do a 3 table join.
    stmt = select(employees.c.first_name,
                  employees.c.last_name,
                  teams.c.name
                  ).select_from(
                      employees
                  ).join(
                      employees_teams,
                      employees.c.id == employees_teams.c.employee_id
                  ).join(
                      teams,
                      teams.c.id == employees_teams.c.team_id
                  ).order_by(teams.c.name.asc()); 
    with engine.connect() as conn:
        results = conn.execute(stmt)
        for row in results:
            first_name = row[0]
            last_name  = row[1]
            team_name  = row[2]
            print("\t" + team_name + ": " + first_name + " " + last_name)

main()
