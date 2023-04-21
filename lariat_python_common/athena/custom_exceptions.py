REPORT_EXCEPTION = "ReportException"


class AthenaQueryRunException(Exception):
    def __init__(self, query_execution_id, exception_type):
        self.query_execution_id = query_execution_id
        self.exception_type = exception_type
