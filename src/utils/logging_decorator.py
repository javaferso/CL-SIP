import logging 
from functools import wraps

# Example from: https://www.geeksforgeeks.org/create-an-exception-logging-decorator-in-python/

def create_logger():
    """
    Creates a logger object and configures it with a file handler to log exceptions to a file.

    Returns:
        logging.Logger: The configured logger object.
    """
	
    # create a logger object 
    logger = logging.getLogger('exc_logger') 
    logger.setLevel(logging.INFO) 

    # create a file to store all the 
    # logged exceptions 
    logfile = logging.FileHandler('exc_logger.log') 

    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt) 

    logfile.setFormatter(formatter) 
    logger.addHandler(logfile) 

    return logger 

logger = create_logger() 

# you will find a log file 
# created in a given path 
print(logger) 

def exception(logger):
    """
    Creates a decorator that logs any exceptions raised by the decorated function.

    Args:
        logger (logging.Logger): The logger to use for logging exceptions.

    Returns:
        function: A decorator function.
    """
    def decorator(func):
        """
        Decorator function that logs exceptions raised by the wrapped function.

        Args:
            func (function): The function to decorate.

        Returns:
            function: The wrapped function.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            """
            Wrapper function that logs exceptions raised by the wrapped function.

            Args:
                *args: The positional arguments to pass to the wrapped function.
                **kwargs: The keyword arguments to pass to the wrapped function.

            Returns:
                The return value of the wrapped function.
            """
            try:
                return func(*args, **kwargs)
            except:
                issue = "exception in "+func.__name__+"\n"
                issue = issue+"=============\n"
                logger.exception(issue)
                raise
        return wrapper
    return decorator 