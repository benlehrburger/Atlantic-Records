from datetime import datetime, timedelta
import pandas as pd


# NORMALIZE THE TIMESERIES DATA

# Take the most recent datapoint and fix other data points to exactly 24 hours after one another
# Rewrite the normalized timeseries data to a new dataframe
def normalize_timeseries(timeseries, aggregate):
    filtered_timeseries = timeseries.copy(deep=True)
    id_series = list(aggregate['TIKTOK_SOUND_SODATONE_ID'])
    filtered_timeseries = filtered_timeseries.loc[((filtered_timeseries['TIKTOK_SOUND_ID'].isin(id_series)))]
    filtered_timeseries = filtered_timeseries.reset_index(drop=True)

    columns = filtered_timeseries.columns
    normalized_timeseries_df = pd.DataFrame(columns=columns)

    for index, sound in filtered_timeseries.iterrows():

        current_sound = filtered_timeseries['TIKTOK_SOUND_ID'].iloc[index]

        if index == len(filtered_timeseries) - 1 or filtered_timeseries['TIKTOK_SOUND_ID'].iloc[
            index + 1] != current_sound:

            most_recent_timestamp = filtered_timeseries['CREATED_AT'].iloc[index]
            most_recent_posts = filtered_timeseries['POST_COUNT'].iloc[index]
            days_since = 1

            normalized_timeseries_df = normalized_timeseries_df.append(
                {"TIKTOK_SOUND_ID": current_sound, "CREATED_AT": most_recent_timestamp,
                 "POST_COUNT": most_recent_posts}, ignore_index=True)

            for i in range(0, index + 1):

                sub_index = index - i
                current_timestamp = filtered_timeseries['CREATED_AT'].iloc[sub_index]

                # Stop after 30 days from the most recent timestamp
                if (most_recent_timestamp - current_timestamp).days > 30:
                    break

                previous_timestamp = filtered_timeseries['CREATED_AT'].iloc[sub_index - 1]
                timeframe = (current_timestamp - previous_timestamp).total_seconds() / 3600

                # Stop normalizing if there is more than a 5 day gap between datapoints
                if abs(timeframe) > 120:
                    break

                else:

                    current_posts = filtered_timeseries['POST_COUNT'].iloc[sub_index]
                    previous_posts = filtered_timeseries['POST_COUNT'].iloc[sub_index - 1]

                    post_delta = current_posts - previous_posts

                    timestamp_we_want = most_recent_timestamp - timedelta(days=days_since)

                    while current_timestamp >= timestamp_we_want and previous_timestamp < timestamp_we_want:

                        hours_since = ((timestamp_we_want - previous_timestamp).total_seconds() / 3600)

                        if hours_since >= 24:

                            days = hours_since // 24
                            hours = hours_since % 24
                            hours_since = (days * 24) + hours

                        else:
                            hours_since = hours_since % 24

                        posts = previous_posts + round((post_delta / timeframe) * hours_since)

                        normalized_timeseries_df = normalized_timeseries_df.append(
                            {"TIKTOK_SOUND_ID": current_sound, "CREATED_AT": timestamp_we_want, "POST_COUNT": posts},
                            ignore_index=True)

                        days_since += 1
                        timestamp_we_want = most_recent_timestamp - timedelta(days=days_since)

    return normalized_timeseries_df


# Check to see if two datapoints are within 24 hours of one another
# Helper method for normalize_timeseries()
def is_within_24_hours(d1, d2=None):
    if d2 is not None:
        if (d1 - d2).total_seconds() <= 86400 or (d2 - d1).total_seconds() <= 86400:
            return True
    else:
        if datetime.now() - timedelta(hours=24) <= d1 <= datetime.now():
            return True

    return False


# Update the aggregate dataframe post deltas with normalized timeseries data
# Helper method for apply_normalized_timeseries()
def update_post_delta(sound, timestamps, first_stamp, mutable_df):
    columns_to_update = ['POST_COUNT_1_DAY_DELTA', 'POST_COUNT_2_DAY_DELTA', 'POST_COUNT_3_DAY_DELTA',
                         'POST_COUNT_7_DAY_DELTA', 'POST_COUNT_14_DAY_DELTA']
    hours_to_update = [24.0, 48.0, 72.0, 168.0, 336.0]

    if not is_within_24_hours(first_stamp):
        for column in columns_to_update:
            with pd.option_context('mode.chained_assignment', None):
                mutable_df[column].loc[mutable_df.TIKTOK_SOUND_SODATONE_ID == sound] = float('NaN')
    else:

        for i in range(0, len(columns_to_update)):

            hours = hours_to_update[i]
            column = columns_to_update[i]

            if hours in timestamps:
                if mutable_df[column].loc[mutable_df.TIKTOK_SOUND_SODATONE_ID == sound].values[0] != timestamps[hours]:
                    with pd.option_context('mode.chained_assignment', None):
                        mutable_df[column].loc[mutable_df.TIKTOK_SOUND_SODATONE_ID == sound] = timestamps[hours]
            else:
                with pd.option_context('mode.chained_assignment', None):
                    mutable_df[column].loc[mutable_df.TIKTOK_SOUND_SODATONE_ID == sound] = float('NaN')


# Update aggregate dataframe week over week growth with normalized timeseries data
# Helper method for apply_normalized_timeseries()
def update_week_over_week_growth(sound, timestamps, first_stamp, mutable_df):
    if 336.0 in timestamps and is_within_24_hours(first_stamp):

        most_recent_week = timestamps[168.0]
        the_week_before = timestamps[336.0] - most_recent_week

        wow_growth = 0

        if the_week_before != 0:
            wow_growth = (most_recent_week - the_week_before) / the_week_before

        with pd.option_context('mode.chained_assignment', None):
            mutable_df['weekOverWeekGrowth'].loc[mutable_df.TIKTOK_SOUND_SODATONE_ID == sound] = wow_growth
    else:

        with pd.option_context('mode.chained_assignment', None):
            mutable_df['weekOverWeekGrowth'].loc[mutable_df.TIKTOK_SOUND_SODATONE_ID == sound] = float('NaN')


# Update the aggregate dataframe with normalized timseries data
def apply_normalized_timeseries(normalized_timeseries_df, mutable_df):
    current_sound = 0
    sounds = []

    for index, sound in normalized_timeseries_df.iterrows():

        if sound['TIKTOK_SOUND_ID'] != current_sound:

            original_sound = sound['TIKTOK_SOUND_ID']
            sounds.append(original_sound)
            original_timestamp = sound['CREATED_AT']
            original_post_count = sound['POST_COUNT']

            next_sound_id = normalized_timeseries_df['TIKTOK_SOUND_ID'].iloc[index + 1]
            next_sound_timestamp = normalized_timeseries_df['CREATED_AT'].iloc[index + 1]
            next_post_count = normalized_timeseries_df['POST_COUNT'].iloc[index + 1]

            i = 1
            timestamps = {}

            while original_sound == next_sound_id and index + i < len(normalized_timeseries_df) - 1:
                timestamps[(
                                       original_timestamp - next_sound_timestamp).total_seconds() / 3600] = original_post_count - next_post_count

                i += 1

                next_sound_id = normalized_timeseries_df['TIKTOK_SOUND_ID'].iloc[index + i]
                next_sound_timestamp = normalized_timeseries_df['CREATED_AT'].iloc[index + i]
                next_post_count = normalized_timeseries_df['POST_COUNT'].iloc[index + i]

            pd.reset_option('mode.chained_assignment')
            with pd.option_context('mode.chained_assignment', None):
                mutable_df['POST_COUNT'].loc[
                    mutable_df.TIKTOK_SOUND_SODATONE_ID == original_sound] = original_post_count

            update_post_delta(original_sound, timestamps, original_timestamp, mutable_df)
            update_week_over_week_growth(original_sound, timestamps, original_timestamp, mutable_df)

        current_sound = sound['TIKTOK_SOUND_ID']

    return mutable_df
