
import os
from pydantic import BaseModel, validator
from dotenv import dotenv_values
import psycopg2
from dagster import asset, Definitions
from make_client.make_client import make_client
from google.ads.googleads.errors import GoogleAdsException


# Define the Pydantic model
class AdData(BaseModel):
    ad_id: int
    ad_type: str
    ad_name: str
    final_urls: list
    headline_part1: str
    headline_part2: str
    description: str
    image_url: str
    impressions: int
    clicks: int
    cost_micros: int

    @validator('final_urls', pre=True)
    def validate_final_urls(cls, v):
        if not isinstance(v, list):
            raise ValueError('final_urls must be a list')
        return v


# Fetch data from Google Ads API
def fetch_google_ads_data(account_id, mcc_id=""):
    client = make_client(mcc_id)
    ga_service = client.get_service("GoogleAdsService", version="v17")

    query = """
    SELECT
        ad_group_ad.ad.id,
        ad_group_ad.ad.type,
        ad_group_ad.ad.name,
        ad_group_ad.ad.final_urls,
        ad_group_ad.ad.expanded_text_ad.headline_part1,
        ad_group_ad.ad.expanded_text_ad.headline_part2,
        ad_group_ad.ad.expanded_text_ad.description,
        ad_group_ad.ad.image_ad.image_url,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros
    FROM
        ad_group_ad
    WHERE
        ad_group_ad.status = 'ENABLED'
        AND ad_group_ad.ad.name LIKE '%bakery%'
    ORDER BY
        metrics.clicks DESC
    LIMIT 10
    """
    search_request = client.get_type("SearchGoogleAdsStreamRequest", version="v17")
    search_request.customer_id = account_id
    search_request.query = query

    stream = ga_service.search_stream(search_request)

    ad_data_list = []
    for batch in stream:
        for row in batch.results:
            ad_data = AdData(
                ad_id=row.ad_group_ad.ad.id,
                ad_type=row.ad_group_ad.ad.type,
                ad_name=row.ad_group_ad.ad.name,
                final_urls=row.ad_group_ad.ad.final_urls,
                headline_part1=row.ad_group_ad.ad.expanded_text_ad.headline_part1,
                headline_part2=row.ad_group_ad.ad.expanded_text_ad.headline_part2,
                description=row.ad_group_ad.ad.expanded_text_ad.description,
                image_url=row.ad_group_ad.ad.image_ad.image_url,
                impressions=row.metrics.impressions,
                clicks=row.metrics.clicks,
                cost_micros=row.metrics.cost_micros,
            )
            ad_data_list.append(ad_data)
    return ad_data_list


# Upload validated data to PostgreSQL
def upload_to_postgres(ad_data_list):
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO ads_data (ad_id, ad_type, ad_name, final_urls, headline_part1, headline_part2, description, image_url, impressions, clicks, cost_micros)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for ad_data in ad_data_list:
        cursor.execute(insert_query, (
            ad_data.ad_id, ad_data.ad_type, ad_data.ad_name, ad_data.final_urls,
            ad_data.headline_part1, ad_data.headline_part2, ad_data.description,
            ad_data.image_url, ad_data.impressions, ad_data.clicks, ad_data.cost_micros
        ))
    conn.commit()
    cursor.close()
    conn.close()


# Define Dagster assets
@asset
def fetch_data_from_google_ads():
    config = dotenv_values("../.env")
    account_id = config["account_id"]
    mcc_id = config["mcc_id"]
    return fetch_google_ads_data(account_id, mcc_id)


@asset
def validate_and_upload_data(fetch_data_from_google_ads):
    ad_data_list = fetch_data_from_google_ads
    upload_to_postgres(ad_data_list)
    return "Data uploaded successfully"


defs = Definitions(
    assets=[fetch_data_from_google_ads, validate_and_upload_data],
)
