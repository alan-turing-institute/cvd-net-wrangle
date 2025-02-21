import sqlalchemy
import getpass
import sys

def connect_database(username=input("Your PostgreSQL username:"),
                     password=getpass.getpass(prompt="Your password:", stream=None),
                     host='localhost',
                     db='CVD-Net'):
    """
    Establishes a connection engine to connect to the CVD-Net database
    
    Parameters
    ----------
    username: str
        PostgreSQL username to be entered as terminal input
    password: str
        PostgreSQL password to be entered as terminal input (not echoed)
    host: str, default 'localhost'
        The host of PostgreSQL server
    db: str, default 'CVD-Net'
        The database name on the PostgreSQL server
    
    Returns
    -------
    engine
        A sqlalchemy engine
    """

    connection_string = f'postgresql+psycopg2://{username}:{password}@{host}/{db}'
    engine = sqlalchemy.create_engine(url=connection_string)

    return engine

def user_choice(question):
    """
    Function for requesting user input to a question with a 'Y' or 'N' response

    Parameters
    ----------
    question: str
        A question to ask the user. For formatting reasons, it should ideally end with a question mark (or full stop)

    Returns
    -------
    bool
        True for when the user answered 'Y' and False for when the user answered 'N'
    """
    question_string = question + ' Please enter \'Y\' or \'N\'.' 

    answered = False
    while answered == False:
        choice = input(question_string).upper()
        if choice == 'Y':
            answered = True
            return True
        elif choice == 'N':
            answered = True
            return False
        else:
            answered = False
            sys.stdout.write("Please ONLY choose from 'Y' or 'N'\n")

