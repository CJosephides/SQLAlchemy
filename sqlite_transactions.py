from sqlalchemy import Table, Column, Integer, create_engine, MetaData, select

engine = create_engine('sqlite:///:memory:', echo=True)
conn = engine.connect()
meta = MetaData(engine)
meta.reflect(engine)

# Make the table if it doesn't exist.
if 'my_table' not in meta.tables:
    Table('my_table', meta,
          Column('PK', Integer, primary_key=True),
          Column('X', Integer)).create()

# Acquire the table.
tab = meta.tables['my_table']

# Example functions that begin their own transactions.
def i_1():
    """ Insert `1`. """

    trans = conn.begin()
    try:
        conn.execute(tab.insert({'X':1}))
        trans.commit()
    except:
        trans.rollback()
        raise

def f_1():
    """ Fail to insert `1`. """

    trans = conn.begin()
    try:
        conn.execute(tab.insert({'X':1}))
        raise Exception
        trans.commit()
    except:
        trans.rollback()
        raise

def i_2():
    """ Insert `2`. """

    trans = conn.begin()
    try:
        conn.execute(tab.insert({'X':2}))
        trans.commit()
    except:
        trans.rollback()
        raise

def f_2():
    """ Fail to insert `2`. """

    trans = conn.begin()
    try:
        conn.execute(tab.insert({'X':2}))
        raise Exception
        trans.commit()
    except:
        trans.rollback()
        raise

# Compositions
def many_all(funcs):
    """
    Execute multiple functions, each possibly with its own transaction,
    inside a transaction wherein all child transactions, as well as
    intermediate logic, must succeed to commit successfully.
    """

    trans = conn.begin()  # parent transaction
    try:
        for func in funcs:
            if (func is Exception) or isinstance(func, Exception):
                raise func
            else:
                func()
        trans.commit()
    except:
        trans.rollback()

def many_any(funcs):
    """
    Execute multiple functions, each possibly with its own transaction,
    and accept as many commits as possible independently of the state 
    of other transactions in the list.
    """

    for func in funcs:
        try:
            if (func is Exception) or isinstance(func, Exception):
                raise func
            else:
                func()  # func is responsible for its own transaction
        except:
            pass

def many_checkpoint(funcs):
    """
    Execute multiple functions, each possibly with its own transaction,
    where failure rolls back to the latest successful commit.
    """

    try:
        for func in funcs:
            if (func is Exception) or isinstance(func, Exception):
                raise func
            else:
                func()
    except:
        pass

# Utilities
def select_all():
    return engine.execute(select([tab])).fetchall()
