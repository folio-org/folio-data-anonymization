import logging
from plugins.users import Users

logger = logging.getLogger(__name__)

data: list = ["circulation", "inventory", "organizations", "users"]


def anonymize(data: list):
    logging.basicConfig(
        filename="anonymizer.log",
        filemode="w",
        format="%(asctime)s %(message)s",
        level=logging.INFO,
    )
    for d in data:
        match d:
            case "users":
                logger.info("Anonymizing users")
                users_task = Users()
                users_task.anonymize_users()
            case _:
                logger.info(f"No anonymizer for {d}.")


if __name__ == "__main__":
    anonymize(data=data)
