class TransformError(Exception):
    def __init__(self, func_name, reason, context=''):
        self.func_name = func_name
        self.reason = reason
        self.context = context
