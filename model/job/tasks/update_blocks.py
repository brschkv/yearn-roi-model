import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sqlalchemy import create_engine

from config import config
from config.config import Config


def get_latest_timestamp(db_engine, config: Config) -> str:
    try:
        with db_engine.connect() as con:
            result = con.execute('SELECT MAX(timestamp) FROM blocks;')
            latest_timestamp = result.first()[0]
            if latest_timestamp is None:
                raise ValueError()
            return latest_timestamp
    except Exception:
        return config.EARLIEST_DATE_WITH_VAULT


def update_blocks(db_engine, config: Config) -> None:
    latest_timestamp = get_latest_timestamp(db_engine, config)
    print(f'Latest Timestamp: {latest_timestamp}')

    print('Connecting to Google Big Query...')
    # This needs the env variable GOOGLE_APPLICATION_CREDENTIALS filled
    # with the path to your credentials file
    credentials, your_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    bqclient = bigquery.Client(credentials=credentials, project=your_project_id,)
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
    print('Successful')

    query_string = f"""
    SELECT
      timestamp,
      number
    FROM 
      `bigquery-public-data.crypto_ethereum.blocks` 
    WHERE 
      timestamp > '{latest_timestamp}'
    ;
    """

    print("Querying Block Information")
    query_result = (
       bqclient.query(query_string)
       .result()
       .to_dataframe(bqstorage_client=bqstorageclient)
    )
    print(f"Succesful, {len(query_result)} new Blocks found!")

    print("Writing Result to Database")
    query_result.to_sql(
        config.BLOCKS_TABLE,
        db_engine,
        if_exists='append',
        index=False
    )
    print("Update completed successfully!")


if __name__ == '__main__':
    db_engine = create_engine(config.DATABASE_CONNECTION)
    update_blocks(db_engine, config)
