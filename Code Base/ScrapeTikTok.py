from bs4 import BeautifulSoup
from json import loads
from sqlalchemy import create_engine
import snowflake.connector
import asyncio
import aiohttp

def scrape_tiktok():

    # in each response body is the following data and more
    # - artist name
    # - song title
    # - total creates
    # - total views
    # - poster followers

    # we can't get a 14-day timeseries for post growth from here (only max 10 days), but we can get it
    # from snowflake
    resps = get_sodatone_artist_tiktok_releases_async_many(MY_SODATONE_ARTIST_IDS)

    # gather all tiktok sound data
    all_tt_sounds = [sound for artist in resps for sound in artist['tiktokSounds']]
    all_tt_sound_ids = list(set([x['id'] for x in all_tt_sounds]))

    # put frontend sodatone data into dataframe for later
    sodatone_frontend_data_df = pd.json_normalize(all_tt_sounds)

    # rename column we will merge by to match same property in snowsql
    sodatone_frontend_data_df = sodatone_frontend_data_df.rename(columns={"id": "TIKTOK_SOUND_SODATONE_ID"})

    # --- GET TIKTOK AGGREGATE WINDOW DATA FROM SNOWFLAKE ---
    # create sql to submit to snowflake

    # when using SQL "IN" filter, Snowflake only allows up to this number per query
    SNOWFLAKE_LIST_ITEM_LIMIT_PER_QUERY = 16_384

    # create queries that capture some subset of total list
    # split into queries for max in filter size
    my_snowflake_sql_queries = [
        get_sql_query_for_sounds(x) for x in group(all_tt_sound_ids, SNOWFLAKE_LIST_ITEM_LIMIT_PER_QUERY)
    ]

    # submit sql
    result_dfs = get_sodatone_query_many_dfs(my_snowflake_sql_queries)

    # combine all dataframes from all queries returned
    snowflake_results = pd.concat(result_dfs)
    snowflake_results.drop_duplicates(inplace=True)

    # merge all results together
    results = snowflake_results.merge(sodatone_frontend_data_df, on='TIKTOK_SOUND_SODATONE_ID', how='left')

    # --- GET TIKTOK SOUND TIMESERIES DATA FROM SNOWFLAKE ---
    # get timeseries data for posts

    # create sql statements
    my_snowflake_sql_queries_timeseries = [
        get_sql_query_for_sound_timeseries_many(x) for x in group(all_tt_sound_ids, SNOWFLAKE_LIST_ITEM_LIMIT_PER_QUERY)
    ]

    # submit sql
    timeseries_dfs = get_sodatone_query_many_dfs(my_snowflake_sql_queries_timeseries)

    # combine dataframes into single dataframe
    timeseries_dfs = pd.concat(timeseries_dfs)

    return results, timeseries_dfs