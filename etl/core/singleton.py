def Singleton(cls):
    """
    The Singleton Design Pattern. Each execution of the application can
    only create one instance of this class
    """
    instances = {}
    def getinstance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args)
        return instances[cls]
    return getinstance
