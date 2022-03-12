from ScrapeTikTok import scrape_tiktok
from DataHandling import blueprint_mutable_df
from NormalizeTimeseries import normalize_timeseries, apply_normalized_timeseries
from Predict import add_aggregate_metrics
from SoundLevel import sound_level
from SongLevel import song_level
from SoundCategorizer import categorize
import schedule
import time

# MAIN EXECUTABLE FUNCTION

def main():
    # Retrieve and parse data
    sound_aggregate_df, sound_timeseries_df = scrape_tiktok()
    mutable_df = blueprint_mutable_df(sound_aggregate_df)
    normalized_timeseries = normalize_timeseries(sound_timeseries_df, mutable_df)
    mutable_df = apply_normalized_timeseries(normalized_timeseries, mutable_df)
    mutable_df = add_aggregate_metrics(mutable_df, normalized_timeseries)

    # Write data to Google Sheets
    sound_df = sound_level(mutable_df)
    song_level(sound_aggregate_df)
    categorize(sound_df)


# UPDATE GOOGLE SHEETS WITH CRON JOB
# Every day at 9:00 AM

schedule.every().day.at("09:00").do(main())

status = True

while status:
    schedule.run_pending()
    time.sleep(1)
