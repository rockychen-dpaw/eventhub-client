import time
import traceback

def repeat_if_failed(retry_interval=100,retry=5,f_is_failed=None,retry_message=None):
    """
    repeat execute the method if some exception is thrown during executing or f_is_failed(result) return True if f_is_failed is not None.
    retry: retry times, -1 means alwasy retry
    retry_interval: the waiting milliseconds between retry
    retry_message: print to stdout before retry, have three positioning paramters.1. current retry times. 2. total retry times. 3. retry interval(milliseconds)
    """
    def _decrator(func):
        _retry_interval = retry_interval / 1000.0
        _retry = retry
        _retry_message = retry_message
        _f_is_failed = f_is_failed
        _func = func

        def _wrapper(*args,**kwargs):
            times = 0
            while True:
                #can run up to retry times plus 1
                try:
                    result = _func(*args,**kwargs)
                    if _f_is_failed and _f_is_failed(result):
                        if times >= _retry and _retry >= 0:
                            #already retry specified retry times
                            return result
                    else:
                        return result
                except KeyboardInterrupt:
                    raise
                except:
                    if times >= _retry and _retry >= 0:
                        #already retry specified retry times
                        raise
                    traceback.print_exc()

                times += 1
                try:
                    if _retry_message:
                        print(_retry_message.format(times,_retry,int(_retry_interval * 1000)))
                    time.sleep(_retry_interval)
                except:
                    #interrupted
                    raise
        return _wrapper

    return _decrator

