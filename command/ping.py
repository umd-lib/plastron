from classes.exceptions import FailureException

def run(fcrepo, args):
    try:
        fcrepo.test_connection()
    except:
        raise FailureException()
