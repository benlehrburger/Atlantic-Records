import pandas as pd
from DataHandling import pandas_to_sheets, scope
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# SOUND CATEGORIZER

credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

gc = gspread.authorize(credentials)


# Categorize sounds into different types of sounds we want to keep an eye on
# Categories: Hot Sounds, Explosive Growers, Slow and Steady Growers, Established and Healthy, Bubbling Under

# HELPER METHODS

# Get a new sound dataframe that we can mutate
def get_blueprint(sound_df):
    old_df = sound_df.copy(deep=True)
    old_df = old_df.drop('1 Day Post Growth Rank', 1)
    old_df = old_df.drop('Change in 1 Day Post Growth Rank', 1)
    filtered_df = pd.DataFrame(columns=list(old_df.columns))

    return old_df, filtered_df


# Check if sound has more total posts than specified threshold
def total_post_filter(sound, lower_lim):
    if sound['Total Post Count'] > lower_lim:
        return True

    return False


# Check if sound has less total posts than specified threshold
def total_post_filter_upper(sound, upper_lim):
    if sound['Total Post Count'] < upper_lim:
        return True

    return False


# Check if sound has more posts in the last day than specified threshold
def one_day_delta_filter(sound, lower_lim):
    if sound['Post Count 1 Day Delta'] > lower_lim:
        return True

    return False


# Check if sound has more posts in the last three days than specified threshold
def three_day_delta_filter(sound, lower_lim):
    if sound['Post Count 3 Day Delta'] > lower_lim:
        return True

    return False


# Check if sound has more posts in the last seven days than specified threshold
def seven_day_delta_filter(sound, lower_lim):
    if sound['Post Count 7 Day Delta'] > lower_lim:
        return True

    return False


# Check if sound has a higher 7D/14D percentage than specified threshold
# Or if that stat is NaN because it's a new sound
def seven_over_fourteen_filter(sound, lower_lim):
    if sound['7 Day Delta Over 14 Day Delta'] > lower_lim or math.isnan(sound['7 Day Delta Over 14 Day Delta']):
        return True

    return False


# Check if sound has a higher 3D/7D percentage than specified threshold
# Or if that stat is NaN because it's a new sound
def three_over_seven_filter(sound, lower_lim):
    if sound['3 Day Delta Over 7 Day Delta'] > lower_lim or math.isnan(sound['3 Day Delta Over 7 Day Delta']):
        return True

    return False


# Check if sound has a higher 2D/3D percentage than specified threshold
# Or if that stat is NaN because it's a new sound
def two_over_three_filter(sound, lower_lim):
    if sound['2 Day Delta Over 3 Day Delta'] > lower_lim or math.isnan(sound['2 Day Delta Over 3 Day Delta']):
        return True

    return False


# Check if sound has a higher week over week growth than specified threshold
# Or if that stat is NaN because it's a new sound
def week_over_week_filter(sound, lower_lim, isnan=None):
    if isnan is None:
        if sound['Week Over Week Growth'] > lower_lim:
            return True
    else:
        if sound['Week Over Week Growth'] > lower_lim or math.isnan(sound['Week Over Week Growth']):
            return True

    return False


# SOUND CATEGORIES

# Category: Explosive Growers
# Total posts > 5000
# Post Count 1D > 1000
# Post Count 7D > 3000
# 7D/Total Posts > 0.1
# 7D/14D > 0.55 or NaN
# 3D/7D > 0.4
# 2D/3D > 0.4
def explosive_grower(workbook, sound_df):
    old_df, explosive_growers = get_blueprint(sound_df)

    for index, sound in old_df.iterrows():

        seven_over_total = sound['7 Day Delta Over Total Posts']

        if ((((total_post_filter(sound, 5000)) and (one_day_delta_filter(sound, 1000))) and (
                seven_day_delta_filter(sound, 3000))) and (
                    (seven_over_fourteen_filter(sound, 0.55) and three_over_seven_filter(
                        sound, 0.4)) and two_over_three_filter(sound, 0.4))) and seven_over_total > 0.1:
            
            explosive_growers.loc[old_df.index[index]] = old_df.iloc[index]

    pandas_to_sheets(explosive_growers, workbook.get_worksheet(1))


# Category: Slow and Steady
# Post Count 1D > 300
# Post Count 7D > 1000
# Post Count 14D > 1500
# Week over Week Growth > 10%
def slow_and_steady(workbook, sound_df):
    old_df, slow_and_steadies = get_blueprint(sound_df)

    for index, sound in old_df.iterrows():

        one_day_delta = sound['Post Count 1 Day Delta']
        fourteen_day_delta = sound['Post Count 14 Day Delta']

        if ((one_day_delta > 300 and fourteen_day_delta > 1500) and seven_day_delta_filter(sound, 1000)) and \
                week_over_week_filter(sound, 10):

            slow_and_steadies.loc[old_df.index[index]] = old_df.iloc[index]

    pandas_to_sheets(slow_and_steadies, workbook.get_worksheet(2))


# Category: Established and Healthy
# Total Posts > 100K
# Post Count 3D > 2500
# Week over Week Growth > -10%
def established_and_healthy(workbook, sound_df):
    old_df, established_and_healthies = get_blueprint(sound_df)

    for index, sound in old_df.iterrows():

        if (total_post_filter(sound, 100000) and three_day_delta_filter(sound, 2500)) and \
                week_over_week_filter(sound, -10):

            established_and_healthies.loc[old_df.index[index]] = old_df.iloc[index]

    pandas_to_sheets(established_and_healthies, workbook.get_worksheet(3))


# Category: Bubbling Under
# Total Posts < 100K
# Total Posts > 2000
# Post Count 1D > 100
# Post Count 3D > 600
# 3D/7D > 0.4
# 2D/3D > 0.5
# Week over Week Growth > 0 or NaN
def bubbling_under(workbook, sound_df):
    old_df, bubbling_unders = get_blueprint(sound_df)

    for index, sound in old_df.iterrows():

        one_day_delta = sound['Post Count 1 Day Delta']

        if (((((total_post_filter(sound, 2000) and total_post_filter_upper(sound, 100000)) and one_day_delta > 100) and
              three_day_delta_filter(sound, 600)) and three_over_seven_filter(sound, 0.4)) and
                two_over_three_filter(sound, 0.5)) and week_over_week_filter(sound, 0):

            bubbling_unders.loc[old_df.index[index]] = old_df.iloc[index]

    pandas_to_sheets(bubbling_unders, workbook.get_worksheet(4))


# Category: All Sounds
# Post Count 7D or 3D or 2D > 100
def all_sounds(workbook, sound_df):
    old_df, all_sound = get_blueprint(sound_df)

    for index, sound in old_df.iterrows():

        one_day_delta = sound['Post Count 1 Day Delta']
        two_day_delta = sound['Post Count 2 Day Delta']
        three_day_delta = sound['Post Count 3 Day Delta']
        seven_day_delta = sound['Post Count 7 Day Delta']

        if ((seven_day_delta > 100 or three_day_delta > 100) or two_day_delta > 100) or one_day_delta > 100:
            all_sound.loc[old_df.index[index]] = old_df.iloc[index]

    pandas_to_sheets(all_sound, workbook.get_worksheet(5))


# Update each category in the Sound Categorizer
def categorize(sound_df):
    workbook = gc.open_by_key("")

    explosive_grower(workbook, sound_df)
    slow_and_steady(workbook, sound_df)
    established_and_healthy(workbook, sound_df)
    bubbling_under(workbook, sound_df)
    all_sounds(workbook, sound_df)
