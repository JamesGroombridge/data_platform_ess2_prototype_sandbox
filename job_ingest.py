import logging
import utility_api_linz
from observability_logging import DeltaTableHandler


logger = logging.getLogger("TableLogger")
handler = DeltaTableHandler("default.application_logs")
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def job_ingest():
    try:
        logger.info("job_ingest start")
        response = utility_api_linz.utility_api_linz_get('2026-04-20','2026-04-26', 'layer-113968-changeset')
        print(response['data'])
        logger.info("job_ingest end")
    except Exception as e:
        logger.info(e)

if __name__ == "__main__":
    job_ingest()