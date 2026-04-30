import requests
import logging

# Just get the logger - don't add handlers (handler is added in job_ingest.py)
logger = logging.getLogger("TableLogger")


def utility_api_linz_get(linz_from, linz_to, layer):
    try:
        logger.info("api_linz_get start")
        response = requests.get(
            f"https://data.linz.govt.nz/services;key=15686062241f4239bfe1f34b3b31aba9/wfs/?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&viewparams=from:{linz_from};to:{linz_to}&count=4&outputformat=json&typeName={layer}"
        )
        response.raise_for_status()  # Raise exception for bad HTTP status
        logger.info("api_linz_get end: status " +  str(response.status_code))
        return {"status": response.status_code, "data": response.json()}
    except Exception as e:
        print(f"Error: {e}")
