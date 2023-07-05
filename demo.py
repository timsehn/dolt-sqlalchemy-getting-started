from sqlalchemy import (
    create_engine,
    text,
    Table,
    Column,
    Integer,
    String,
    Date,
    select,
    insert,
    update,
    delete,
    ForeignKey,
    MetaData
)

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from pprint import pprint

def main():
    engine = dolt_checkout('main')
    print_active_branch(engine)

    # Start fresh so we can re-run this script
    reset_database(engine)
    delete_non_main_branches(engine)
    
    # Build our tables
    setup_database(engine)

    print_tables(engine)
            
    # Our first Dolt feature. This will commit the first time
    # But after that nothing has changed so there is nothing to commit.
    dolt_commit(engine, "Tim <tim@dolthub.com>", "Created tables")

    # Examine a Dolt system table: dolt_log
    print_commit_log(engine)

    # Load rows into the tables
    insert_data(engine)

    print_summary_table(engine)

    # Show off dolt_status and dolt_diff
    print_status(engine)
    print_diff(engine, "employees")

    # Dolt commit our changes
    dolt_commit(engine,
                "Aaron <aaron@dolthub.com>",
                "Inserted data into tables")

    print_commit_log(engine)

    # Show off dolt_reset
    drop_table(engine, "employees_teams")
    print_status(engine)
    print_tables(engine)
    dolt_reset_hard(engine, None)
    print_status(engine)
    print_tables(engine)

    # Show off branch and merge
    dolt_create_branch(engine, 'modify_data')
    engine = dolt_checkout('modify_data')
    print_active_branch(engine)
    modify_data(engine)
    print_status(engine)
    print_diff(engine, 'employees')
    print_diff(engine, 'employees_teams')
    print_summary_table(engine)
    dolt_commit(engine, 'Brian <brian@dolthub.com>', 'Modified data on branch')

    # Switch back to main because I want the same merge base
    dolt_checkout('main')
    dolt_create_branch(engine, 'modify_schema')
    engine = dolt_checkout('modify_schema')
    print_active_branch(engine)
    modify_schema(engine)
    print_status(engine)
    print_diff(engine, "employees")
    

def reset_database(engine):
    metadata_obj = MetaData()

    # Here we find the first commit in the log and reset to that commit
    dolt_log = Table("dolt_log", metadata_obj, autoload_with=engine)
    stmt = select(dolt_log.c.commit_hash).limit(1).order_by(dolt_log.c.date.asc())
    with engine.connect() as conn:
        results_obj = conn.execute(stmt)
        results = results_obj.fetchall()
        init_commit_hash = results[0][0]

        dolt_reset_hard(engine, init_commit_hash)

def delete_non_main_branches(engine):
    metadata_obj = MetaData()

    # Iterate through the non-main branches and delete them with
    # CALL DOLT_BRANCH('-D', '<branch>'). '-D' force deletes just in
    # case I have some unmerged modifications from a failed run.
    dolt_branches = Table("dolt_branches", metadata_obj, autoload_with=engine)
    stmt = select(dolt_branches.c.name).where(dolt_branches.c.name != 'main')
    with engine.connect() as conn:
        results = conn.execute(stmt)
        for row in results:
            branch = row[0];
            print("Deleting branch: " + branch)
            stmt = text("CALL DOLT_BRANCH('-D', '" + branch + "')")
            conn.execute(stmt)
        
def setup_database(engine):
    metadata_obj = MetaData()

    # This is standard SQLAlchemy without the ORM
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
    (employees, teams, employees_teams) = load_tables(engine)
    
    # This is standard SQLAlchemy
    stmt = insert(employees).values([
        {'id':0, 'last_name':'Sehn', 'first_name':'Tim'}, 
        {'id':1, 'last_name':'Hendriks', 'first_name':'Brian'}, 
        {'id':2, 'last_name':'Son', 'first_name':'Aaron'}, 
        {'id':3, 'last_name':'Fitzgerald', 'first_name':'Brian'}
        ])
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

def modify_data(engine):
    (employees, teams, employees_teams) = load_tables(engine)

    update_stmt = update(employees).where(employees.c.first_name == 'Tim'
                                          ).values(first_name='Timothy')
    
    insert_emp_stmt = insert(employees).values([
        {'id':4, 'last_name':'Wilkins', 'first_name':'Daylon'}
        ])
    insert_et_stmt = insert(employees_teams).values([
        {'employee_id':4, 'team_id':0}
    ])

    delete_stmt = delete(employees_teams).where(
        employees_teams.c.employee_id == 0
    ).where(employees_teams.c.team_id == 1)
    
    with engine.connect() as conn:
        conn.execute(update_stmt)
        conn.execute(insert_emp_stmt)
        conn.execute(insert_et_stmt)
        conn.execute(delete_stmt)
        conn.commit()

def modify_schema(engine):
    (employees, teams, employees_teams) = load_tables(engine)

    # SQLAlchemy does not support table alters so we use text
    stmt = text('alter table employees add column start_date date')
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()
    
    # Update using the SQL Alchemy session interface
    class Base(DeclarativeBase):
        pass

    class Employee(Base):
        __tablename__ = "employees"
        id: Mapped[int] = mapped_column(primary_key=True)
        last_name: Mapped[str] = mapped_column(String(255))
        first_name: Mapped[str] = mapped_column(String(255))
        start_date: Mapped[Date] = mapped_column(Date)

        def __repr__(self) -> str:
            return f"Employee(id={self.id!r}, last_name={self.last_name!r}, first_name={self.first_name!r}, start_date={self.start_date!r})"

    session = Session(engine)
    Tim = session.get(Employee, 0)
    Tim.start_date = "2018-08-06"

    Aaron = session.get(Employee, 1)
    Aaron.start_date = "2018-08-06"

    BHeni = session.get(Employee, 2)
    BHeni.start_date = "2018-08-06"

    Fitz = session.execute(select(Employee).filter_by(last_name="Fitzgerald")).scalar_one()
    Fitz.start_date = "2021-04-19"
    
    session.commit()
    
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

def dolt_reset_hard(engine, commit):
    if ( commit ):
        stmt = text("CALL DOLT_RESET('--hard', '" + commit + "')")
        print("Resetting to commit: " + commit)
    else:
        stmt = text("CALL DOLT_RESET('--hard')")
        print("Resetting to HEAD")


    with engine.connect() as conn:
        results = conn.execute(stmt)
        conn.commit()

def dolt_create_branch(engine, branch):
    # Check if branch exists
    metadata_obj = MetaData()

    dolt_branches = Table("dolt_branches", metadata_obj, autoload_with=engine)
    stmt = select(dolt_branches.c.name).where(dolt_branches.c.name == branch)
    with engine.connect() as conn:
        results = conn.execute(stmt)
        rows = results.fetchall()
        if ( len(rows) > 0 ):
             print("Branch exists: " + branch)
             return

    # Create branch
    stmt = text("CALL DOLT_BRANCH('" + branch + "')")
    with engine.connect() as conn:
        results = conn.execute(stmt)
        print("Created branch: " + branch)

def dolt_checkout(branch):
    engine_base = "mysql+mysqlconnector://root@127.0.0.1:3306/sql_alchemy_big_demo"
    # Branches can be "checked out" via connection string. We make heavy use
    # of reflection in this example for system tables so passing around an
    # engine instead of a connection is best for this example. We'll
    # also show how to checkout a branch using call dolt_checkout() in
    # the connect to branch function.
    engine = create_engine(
    	engine_base + "/" + branch
    )
    print("Using branch: " + branch)
    return engine

def connect_to_branch(engine, branch):
    engine = create_engine(engine_base)
    stmt = text("CALL DOLT_CHECKOUT('" + branch + "')")
    with engine.connect() as conn:
        conn.execute(stmt)
        return conn
            
def print_commit_log(engine):
    # Examine a dolt system table, dolt_log, using reflection
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

def print_active_branch(engine):
    stmt = text("select active_branch()")
    with engine.connect() as conn:
        results = conn.execute(stmt)
        rows = results.fetchall()
        active_branch = rows[0][0]
        print("Active branch: " + active_branch)
            
def print_diff(engine, table):
    metadata_obj = MetaData()

    print("Diffing table: " + table)
    dolt_diff = Table("dolt_diff_" + table,
                      metadata_obj,
                      autoload_with=engine)

    # Show only working set changes
    stmt = select(dolt_diff).where(dolt_diff.c.to_commit == 'WORKING')
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
