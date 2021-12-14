import logging


class WhereScapeLogHandler(logging.Handler):
    """
    A handler class specific for WhereScape which buffers logging records in
    until the scripts exit. It will then print the correct WhereScape exit
    code (-2, -1 or 1) and subsequently the rest of the logging records. This
    class is inspired by the BufferingHandler.
    """

    def __init__(self, wherescape, level=logging.INFO):
        """
        Initialize the handler with the supplied logging level.
        """
        logging.Handler.__init__(self, level)
        self.buffer = []
        self.highest_level = 0
        self.wherescape = wherescape

    def emit(self, record):
        """
        Emit a record.

        Append the record and determine the highest errorlevel sofar.
        """
        self.buffer.append(record)
        self.highest_level = max(record.levelno, self.highest_level)

    def flush(self):
        """
        Flush is being called on program exit. For WhereScape, this would be
        the actual printing function. WhereScape expects the following return
        codes:

        Prints '1' + success_message + Messages if there are no errors.
        Since success_message is the first line that's printed, it's visible
        in the WhereScape Audit Log (subsequent messages are visible in the
        detail log).

        Prints '-1' + Error/warning messages & Messages if there are warnings.
        Prints '-2' + Error/warning messages & Messages if there are errors.
        """

        self.acquire()
        message_status = ""

        if self.highest_level < 30:
            message_status = "succeeded"
            print(1)
        elif self.highest_level < 40:
            message_status = "succeeded with warnings"
            print(-1)
        elif self.highest_level < 50:
            message_status = "failed"
            print(-2)
        else:
            message_status = "failed miserably"
            print(-3)

        # Print the actual logs, starting with the main message
        if self.wherescape.main_message != "":
            print(self.wherescape.main_message)
        else:
            print(f"{self.wherescape.job_name} {message_status}")

        # Print the rest of the logging messages
        for record in self.buffer:
            print(self.format(record))

        try:
            self.buffer = []
        finally:
            self.release()
        self.output = False


def initialise_wherescape_logging(wherescape):
    """
    Function to configure the logging for WhereScape.
    """

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fmt = "[%(levelname)s] %(asctime)s %(filename)s %(funcName)s():%(lineno)i: %(message)s"
    message_format = logging.Formatter(fmt=fmt, datefmt="%H:%M:%S")

    w_handler = WhereScapeLogHandler(wherescape, level=logging.INFO)
    w_handler.setFormatter(message_format)
    logger.addHandler(w_handler)

    f_handler = logging.FileHandler(f"{wherescape.workdir}wherescape.log")
    f_handler.setFormatter(message_format)
    logger.addHandler(f_handler)
