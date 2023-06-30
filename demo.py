from sqlalchemy import (
    create_engine,
    text,
    Table,
    Column,
    Integer,
    String,
    ForeignKey
)

def main():
    engine = create_engine(
	"mysql+mysqlconnector://root@127.0.0.1:3306/sql_alchemy_big_demo"
    )

    setup_database(engine)

    # Raw SQL here to show what we've done
    with engine.connect() as conn:
        result = conn.execute(text("show tables"))

        print("Tables in database:")
        for row in result:
            table = row[0]
            print("\t" + table)
            
    # Our first Dolt feature. This will commit the first time
    # But after that nothing has changed so there is nothing to commit.
    dolt_commit(engine, "Tim <tim@dolthub.com>", "Created tables")

def setup_database(engine):
    from sqlalchemy import MetaData
    metadata_obj = MetaData()

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

def insert_data(engine):
    pass
    
def dolt_commit(engine, author, message):
    with engine.connect() as conn:
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
    
main()
