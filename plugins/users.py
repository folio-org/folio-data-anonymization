import logging
from folio_data_anonymization.plugins.database import Database

logger = logging.getLogger(__name__)


class Users:
    def __init__(self):
        self.schema = "mod_users"
        self.db = Database()

    def anonymize_users(self) -> bool:
        table_name = self.db.table_name(self.schema, "users")
        logger.info(f"Anonymizing {table_name}")
        return True
