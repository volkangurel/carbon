import importlib
from carbon.conf import settings


# application database
def get_db():
    module_name, class_name = settings.DB_INIT_FUNC.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)()
