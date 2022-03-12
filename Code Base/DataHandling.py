import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Retrieve Sodatone Post Growth stats from CSV files
one_day_growth_10k = pd.read_csv('Sodatone TikTok Post Growth/100K/Large 1-Day Post Growth 10k.csv')
one_day_growth_20k = pd.read_csv('Sodatone TikTok Post Growth/100K/Large 1-Day Post Growth 20k.csv')
consistent_growth_50k = pd.read_csv('Sodatone TikTok Post Growth/100K/Consistent 7-Day Growth 50k.csv')
consistent_growth_100k = pd.read_csv('Sodatone TikTok Post Growth/100K/Consistent 7-Day Growth 100k.csv')
post_milestones_10k = pd.read_csv('Sodatone TikTok Post Growth/100K/10k Post Milestones.csv')

post_milestones_100k = pd.read_csv('Sodatone TikTok Post Growth/1M/100k Post Milestones.csv')
one_day_growth_100k = pd.read_csv('Sodatone TikTok Post Growth/1M/Large 1-Day Post Growth 100k.csv')
one_day_growth_250k = pd.read_csv('Sodatone TikTok Post Growth/1M/Large 1-Day Post Growth 250k.csv')
one_day_growth_500k = pd.read_csv('Sodatone TikTok Post Growth/1M/Large 1-Day Post Growth 500k.csv')

# Google Sheets stuff
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

# GOOGLE SHEETS METHODS

# Google Sheet file IDs
sound_categorizer = ""
sound_level_google_sheet = ""
song_level_google_sheet = ""

credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

gc = gspread.authorize(credentials)

# Insert each value in a dataframe into Sheets
def iter_pd(df):
    for val in df.columns:
        yield val

    for row in df.to_numpy():
        for val in row:
            yield val


# Copy a Pandas dataframe to a Google Sheet
def pandas_to_sheets(pandas_df, sheet, clear=True):
    if clear:
        sheet.clear()

    (row, col) = pandas_df.shape
    cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))

    for cell, val in zip(cells, iter_pd(pandas_df)):
        cell.value = str(val)

    sheet.update_cells(cells)

# Do an initial filter of aggregate sounds dataframe
# postCount7DayDelta > 100 and totalEngagementsViewsRatio < 0.8
# Add new metrics to the dataframe
def blueprint_mutable_df(aggregate):

    mutable_df = aggregate.copy(deep=True)
    mutable_df = mutable_df.loc[((aggregate['postCount7DayDelta'] > 100))]
    mutable_df = mutable_df.loc[((aggregate['totalEngagementsViewsRatio'] < 0.8))]
    mutable_df = mutable_df.reset_index(drop=True)
    mutable_df['CHANCES_OF_REACHING_50K'] = 0.0
    mutable_df['CHANCES_OF_REACHING_100K'] = 0.0
    mutable_df['CHANCES_OF_REACHING_500K'] = 0.0
    mutable_df['CHANCES_OF_REACHING_1M'] = 0.0
    mutable_df['5_DAY_ROLLING_WINDOW'] = None
    mutable_df['5_DAY_GROWTH_RATE'] = None

    return mutable_df
