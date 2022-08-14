from datetime import date
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from DataHandling import pandas_to_sheets, scope

# BUILD THE SONG LEVEL GOOGLE SHEET

credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

gc = gspread.authorize(credentials)

# Accumulate stats across all sounds of each song
# Helper method for build_song_df()
def get_songs(aggregate):
    songs = set()
    sounds_of_songs = {}

    cols = list(aggregate.columns)
    song_lev = pd.DataFrame(columns=cols)

    for index, sound in aggregate.iterrows():

        song_artist = aggregate['fingerprintedArtistName'].iloc[index]
        song_title = aggregate['fingerprintedName'].iloc[index]
        package = (song_title, song_artist)

        if package in songs:
            total_posts = aggregate['POST_COUNT'].iloc[index]
            tiktok_url = aggregate['url'].iloc[index]
            sounds_of_songs[song_title].append((total_posts, tiktok_url))

        else:
            songs.add(package)
            sounds_of_songs[song_title] = []
            total_posts = aggregate['POST_COUNT'].iloc[index]
            tiktok_url = aggregate['url'].iloc[index]
            sounds_of_songs[song_title].append((total_posts, tiktok_url))
            song_lev.loc[aggregate.index[index]] = aggregate.iloc[index]

    return sounds_of_songs, song_lev


# Calculate new metrics: 1D rank, 1D video count change, 7D video count change, top performing sounds
def build_song_df(aggregate):
    sounds_of_songs, song_lev = get_songs(aggregate)

    # Only grab the following columns from the dataframe
    song_df = song_lev[
        ['fingerprintedName', 'fingerprintedArtistName', 'aggregateVideoCount', 'aggregateVideoCount7Days',
         'aggregateVideoViews', 'aggregateVideoEngagements', 'aggregateSoundCount']].copy(deep=True)
    song_df = song_df.reset_index(drop=True)

    sheet = gc.open('Song Level')

    last_pull = sheet.get_worksheet(-1)
    last_pull_records = last_pull.get_all_records()
    last_pull_df = pd.DataFrame.from_dict(last_pull_records)

    one_day_deltas = []
    empties = []

    # Calculate 1D video count stats
    for index, song in song_df.iterrows():

        song_title = song_df['fingerprintedName'].iloc[index]
        song_artist = song_df['fingerprintedArtistName'].iloc[index]

        row = last_pull_df.loc[
            (last_pull_df['Song Title'] == song_title) & (last_pull_df['Song Artist'] == song_artist)]

        if row.empty:

            try:
                row = last_pull_df.loc[
                    (last_pull_df['Song Title'] == float(song_title)) & (last_pull_df['Song Artist'] == song_artist)]
            except ValueError:
                pass

            if row.empty:
                empties.append(index)

        if not row.empty:
            old_video_count = row['Aggregate Video Count'].iloc[0]
            current_video_count = song_df['aggregateVideoCount'].iloc[index]
            one_day_delta = current_video_count - old_video_count
            one_day_deltas.append(one_day_delta)

    for empty_row in empties:
        song_df = song_df.drop(index=empty_row)

    song_df.insert(0, "1 Day Video Count Change", one_day_deltas)

    song_df = song_df.sort_values(by=['1 Day Video Count Change'], ascending=False)
    song_df = song_df.reset_index(drop=True)

    ranks = []
    rank_change = []
    urls = []

    # Calculate video count change stats and top performing sounds
    for index, song in song_df.iterrows():

        current_rank = index + 1
        ranks.append(current_rank)

        song_title = song_df['fingerprintedName'].iloc[index]
        song_artist = song_df['fingerprintedArtistName'].iloc[index]

        row = last_pull_df.loc[
            (last_pull_df['Song Title'] == song_title) & (last_pull_df['Song Artist'] == song_artist)]

        if row.empty:
            row = last_pull_df.loc[
                (last_pull_df['Song Title'] == float(song_title)) & (last_pull_df['Song Artist'] == song_artist)]

        old_rank = row['1 Day Song Growth Rank'].iloc[0]
        rank_delta = old_rank - current_rank
        rank_input = rank_delta

        if rank_delta > 0:
            rank_input = '+' + str(rank_input)

        rank_change.append(rank_input)

        url_options = sounds_of_songs[song_title]

        posts = []
        links = []

        for url in url_options:
            posts.append(url[0])
            links.append(url[1])

        # Get the top 3 best performing sounds for each song
        top3_posts = sorted(zip(posts, links), reverse=True)[:3]
        final_urls = []

        for post in top3_posts:
            final_urls.append(post[1])

        urls.append(final_urls)

    # Add new metrics to dataframe
    song_df.insert(0, "1 Day Song Growth Rank", ranks)
    song_df.insert(1, "Change in 1 Day Song Growth Rank", rank_change)
    song_df["Top Performing Sound URLs"] = urls

    song_df = song_df.rename(columns={"fingerprintedName": "Song Title", "fingerprintedArtistName": "Song Artist",
                                      "aggregateVideoCount": "Aggregate Video Count",
                                      "aggregateVideoCount7Days": "Change in Video Count over Past 7 Days",
                                      "aggregateVideoViews": "Aggregate Video Views",
                                      "aggregateVideoEngagements": "Aggregate Video Engagements",
                                      "aggregateSoundCount": "Aggregate Sound Count"})

    return song_df


# Copy new song dataframe to Google Sheets
def song_level(aggregate):
    song_df = build_song_df(aggregate)

    workbook = gc.open_by_key("1VjP3rw7qowuXOB5INpKKXiGa7bNAsbvQR37igJ7YmfU")

    # Title of sheet is today's date
    sheet_title = str(date.today())
    rows = len(song_df)
    cols = len(list(song_df.columns))
    worksheet = workbook.add_worksheet(title=sheet_title, rows=rows, cols=cols)

    pandas_to_sheets(song_df, workbook.worksheet(sheet_title))
