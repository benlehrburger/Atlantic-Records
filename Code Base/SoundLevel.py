from datetime import date
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from DataHandling import pandas_to_sheets, scope

# BUILD THE SOUND LEVEL GOOGLE SHEET

credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

gc = gspread.authorize(credentials)

# Calculate each sound's rank according to change in post growth over some period of time
# Either 1 day, 3 day, or 7 day post growth change
# Order sounds according to their 1 day post growth rank
# Calculate the change in each sound's rank from the day prior
# Add all metrics to aggregate dataframe and Google Sheets
# Helper method for build_sound_df()
def make_rank(df, old_df, value):
    old_col = 'POST_COUNT_{}_DAY_DELTA'.format(value)
    new_col = '{} Day Post Growth Rank'.format(value)
    change_col = 'Change in {} Day Post Growth Rank'.format(value)

    df2 = df.copy(deep=True)
    df2 = df.sort_values(by=[old_col], ascending=False)
    df2 = df2.reset_index()

    loc = list(df.columns).index(old_col)
    df.insert(loc + 1, new_col, "")
    df.insert(loc + 2, change_col, "")

    for index, sound in df2.iterrows():

        sound_id = sound['TIKTOK_SOUND_SODATONE_ID']
        row = old_df.loc[old_df['Sound Sodatone ID'] == sound_id]

        old_rank = row[new_col]
        rank_delta = old_rank - index + 1

        # No change in rank if the sound did not have a rank yesterday
        if row.empty:
            rank_delta = 'N/A'

        else:

            if len(rank_delta) > 1:
                rank_delta = rank_delta.iloc[0]
            else:
                rank_delta = rank_delta.item()

            if rank_delta > 0:
                rank_delta = '+' + str(rank_delta)

        old_index = df2['index'].loc[index]
        df.iat[old_index, loc + 1] = index + 1
        df.iat[old_index, loc + 2] = rank_delta

    return df


# Calculate new metrics: 1D rank, 3D rank, 7D rank, 7D/total, 7D/14D, 3D/7D, 2D/3D, 1D/3D
def build_sound_df(mutable_df):
    deliverable = mutable_df[
        ['fingerprintedName', 'fingerprintedArtistName', 'url', 'TIKTOK_SOUND_SODATONE_ID', 'POST_COUNT',
         'POST_COUNT_1_DAY_DELTA', 'POST_COUNT_2_DAY_DELTA', 'POST_COUNT_3_DAY_DELTA', 'POST_COUNT_7_DAY_DELTA',
         'POST_COUNT_14_DAY_DELTA', 'weekOverWeekGrowth', 'CHANCES_OF_REACHING_50K', 'CHANCES_OF_REACHING_100K',
         'CHANCES_OF_REACHING_500K', 'CHANCES_OF_REACHING_1M', '5_DAY_GROWTH_RATE']].copy(deep=True)
    deliverable = deliverable.sort_values(by=['POST_COUNT_1_DAY_DELTA'], ascending=False)
    old_indices = deliverable.index
    deliverable = deliverable.reset_index(drop=True)

    sheet = gc.open('Sound Level')

    last_pull = sheet.get_worksheet(-1)
    last_pull_records = last_pull.get_all_records()
    last_pull_df = pd.DataFrame.from_dict(last_pull_records)

    # Get the ranks and rank changes for 1D, 3D, and 7D
    deliverable = make_rank(deliverable, last_pull_df, 1)
    deliverable = make_rank(deliverable, last_pull_df, 3)
    deliverable = make_rank(deliverable, last_pull_df, 7)

    percent_posts_7d = []
    percent_posts_7d_14d = []
    percent_posts_3d_7d = []
    percent_posts_2d_3d = []
    percent_posts_1d_3d = []

    for index, sound in deliverable.iterrows():

        post_count = mutable_df['POST_COUNT'].loc[old_indices[index]]
        one_day_delta = mutable_df['POST_COUNT_1_DAY_DELTA'].loc[old_indices[index]]
        two_day_delta = mutable_df['POST_COUNT_2_DAY_DELTA'].loc[old_indices[index]]
        three_day_delta = mutable_df['POST_COUNT_3_DAY_DELTA'].loc[old_indices[index]]
        seven_day_delta = mutable_df['POST_COUNT_7_DAY_DELTA'].loc[old_indices[index]]
        fourteen_day_delta = mutable_df['POST_COUNT_14_DAY_DELTA'].loc[old_indices[index]]

        # Control for division by 0 or division by NaN
        if seven_day_delta == 0:
            percent_posts_3d_7d.append(float('NaN'))

        # 3D/7D
        else:
            percent_posts_3d_7d.append(three_day_delta / seven_day_delta)

        # Control for division by 0 or division by NaN
        if fourteen_day_delta == 0:
            percent_posts_7d_14d.append(float('NaN'))

        # 7D/14D
        else:
            percent_posts_7d_14d.append(seven_day_delta / fourteen_day_delta)

        # Control for division by 0 or division by NaN
        if three_day_delta == 0:
            percent_posts_2d_3d.append(float('NaN'))
            percent_posts_1d_3d.append(float('NaN'))

        # 2D/3D and 1D/3D
        else:
            percent_posts_2d_3d.append(two_day_delta / three_day_delta)
            percent_posts_1d_3d.append(one_day_delta / three_day_delta)

        # 7D/total
        percent_posts_7d.append(seven_day_delta / post_count)

    deliverable["7 Day Delta Over Total Posts"] = percent_posts_7d
    deliverable["7 Day Delta Over 14 Day Delta"] = percent_posts_7d_14d
    deliverable["3 Day Delta Over 7 Day Delta"] = percent_posts_3d_7d
    deliverable["2 Day Delta Over 3 Day Delta"] = percent_posts_2d_3d
    deliverable["1 Day Delta Over 3 Day Delta"] = percent_posts_1d_3d

    # Rename column headings
    deliverable = deliverable.rename(
        columns={"fingerprintedName": "Song", "fingerprintedArtistName": "Artist", "url": "Sound URL",
                 "TIKTOK_SOUND_SODATONE_ID": "Sound Sodatone ID", "POST_COUNT": "Total Post Count",
                 "POST_COUNT_1_DAY_DELTA": "1D", "POST_COUNT_2_DAY_DELTA": "2D", "POST_COUNT_3_DAY_DELTA": "3D",
                 "POST_COUNT_7_DAY_DELTA": "7D", "POST_COUNT_14_DAY_DELTA": "14D",
                 "CHANCES_OF_REACHING_50K": "Chances of Reaching 50K Posts",
                 "CHANCES_OF_REACHING_100K": "Chances of Reaching 100K Posts",
                 "CHANCES_OF_REACHING_500K": "Chances of Reaching 500K Posts",
                 "CHANCES_OF_REACHING_1M": "Chances of Reaching 1M posts",
                 "weekOverWeekGrowth": "Week Over Week Growth", "5_DAY_GROWTH_RATE": "5 Day Post Count Growth Rate"})

    return deliverable


# Copy new sound dataframe to Google Sheets
def sound_level(mutable_df):
    sound_df = build_sound_df(mutable_df)

    workbook = gc.open_by_key("")

    # Title of sheet is today's date
    sheet_title = str(date.today())
    rows = len(sound_df)
    cols = len(list(sound_df.columns))
    worksheet = workbook.add_worksheet(title=sheet_title, rows=rows, cols=cols)

    pandas_to_sheets(sound_df, workbook.worksheet(sheet_title))

    return sound_df