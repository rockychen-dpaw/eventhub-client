__version__ = '1.0.0'
import ast
import os

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

env_loaded = False
if not env_loaded:
    dot_env = os.path.join(BASE_DIR, ".env")
    if os.path.exists(dot_env) :
        load_dotenv(dotenv_path=dot_env)
        env_loaded = True

def env(key, default=None, required=False,vtype=None):
    """
    Retrieves environment variables and returns Python natives. The (optional)
    default will be returned if the environment variable does not exist.
    """
    try:
        value = os.environ[key]
        value = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        pass
    except KeyError:
        if default is not None or not required:
            return default
        raise Exception("Missing required environment variable '%s'" % key)

    if vtype is None:
        if default is not None:
            vtype = default.__class__

    if vtype is None:
        return value
    elif isinstance(value,vtype):
        return value
    elif issubclass(vtype,list):
        if isinstance(value,tuple):
            return list(value)
        else:
            value = str(value).strip()
            if not value:
                return []
            else:
                return value.split(",")
    elif issubclass(vtype,tuple):
        if isinstance(value,list):
            return tuple(value)
        else:
            value = str(value).strip()
            if not value:
                return tuple()
            else:
                return tuple(value.split(","))
    elif issubclass(vtype,bool):
        value = str(value).strip()
        if not value:
            return False
        elif value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        else:
            raise Exception("'{}' is a boolean environment variable, only accept value 'true' ,'false' and '' with case insensitive, but the configured value is '{}'".format(key,value))
    elif issubclass(vtype,int):
        return int(value)
    elif issubclass(vtype,float):
        return float(value)
    else:
        raise Exception("'{0}' is a {1} environment variable, but {1} is not supported now".format(key,vtype))



