import logging

logging.basicConfig(encoding='utf-8'
                            , level=logging.DEBUG
                            , format=f"[%(levelname)s] %(message)s\n{'-'}\n"
                            , handlers=[logging.FileHandler("logging.log")
                                        # ,logging.StreamHandler()
                                        ]
                            )

# print(logging.__dict__.keys())
print(logging.basicConfig)

# ['__name__', '__doc__', '__package__', '__loader__', '__spec__', '__path__', '__file__', '__cached__', '__builtins__', 'sys', 'os', 'time', 'io', 're', 'traceback', 'warnings', 'weakref', 'collections', 'GenericAlias', 'Template', '__all__', 'threading', '__author__', '__status__', '__version__', '__date__', '_startTime', 'raiseExceptions', 'logThreads', 'logMultiprocessing', 'logProcesses', 'CRITICAL', 'FATAL', 'ERROR', 
# 'WARNING', 'WARN', 'INFO', 'DEBUG', 'NOTSET', '_levelToName', '_nameToLevel', 'getLevelNamesMapping', 'getLevelName', 'addLevelName', 'currentframe', '_srcfile', '_is_internal_frame', '_checkLevel', '_lock', '_acquireLock', '_releaseLock', '_register_at_fork_reinit_lock', 'LogRecord', '_logRecordFactory', 'setLogRecordFactory', 'getLogRecordFactory', 'makeLogRecord', '_str_formatter', 'PercentStyle', 'StrFormatStyle', 'StringTemplateStyle', 'BASIC_FORMAT', '_STYLES', 'Formatter', '_defaultFormatter', 'BufferingFormatter', 'Filter', 'Filterer', '_handlers', '_handlerList', '_removeHandlerRef', '_addHandlerRef', 'Handler', 'StreamHandler', 'FileHandler', '_StderrHandler', '_defaultLastResort', 'lastResort', 'PlaceHolder', 'setLoggerClass', 'getLoggerClass', 'Manager', 'Logger', 'RootLogger', '_loggerClass', 'LoggerAdapter', 'root', 'basicConfig', 'getLogger', 
# 'critical', 'fatal', 'error', 'exception', 'warning', 'warn', 'info', 'debug', 'log', 'disable', 'shutdown', 'atexit', 'NullHandler', '_warnings_showwarning', '_showwarning', 'captureWarnings']