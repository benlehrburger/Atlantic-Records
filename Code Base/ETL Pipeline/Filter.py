import pandas as pd

# CREATE DIFFERENT FILTERS FOR AGGREGATE DATA

# Calculate the probability of each sound reaching 50K, 100K, 500K, and 1M posts
# According to Sodatone TikTok Sound Analysis

# FILTER 1 – ALREADY VIRAL SOUNDS
# Filter sounds that are already above 50K/100K/500K/1M posts
# Those that pass have a 100% chance of reaching 50K/100K/500K/1M posts
def already_viral(sound):
    total_posts = sound['POST_COUNT']

    # 50K < total posts < 100K
    if total_posts >= 50000 and total_posts < 100000:
        return 1.0, 0.0, 0.0, 0.0

    # 100K < total posts < 500K
    elif total_posts >= 100000 and total_posts < 500000:
        return 1.0, 1.0, 0.0, 0.0

    # 500K < total posts < 1M
    elif total_posts >= 500000 and total_posts < 1000000:
        return 1.0, 1.0, 1.0, 0.0

    # total posts > 1M
    elif total_posts >= 1000000:
        return 1.0, 1.0, 1.0, 1.0

    else:
        return 0.0, 0.0, 0.0, 0.0


# FILTER 2 – LARGE 1-DAY POST GROWTH
# Filter sounds that have experienced a large jump in post growth in a one day period
def large_one_day_post_growth(sound):
    daily_change = sound['postCount1DayDelta']
    total_posts = sound['POST_COUNT']
    buckets = 'POSTS/DAY'

    # Chances of making it to 100K posts
    k100 = 0.0
    # Chances of making it to 1M posts
    m1 = 0.0

    # Only consider songs that are increasing in posts
    if daily_change >= 0:

        # total posts < 10K
        if total_posts < 10000:
            k100 = get_post_growth(sound, daily_change, one_day_growth_10k, '100K', buckets)

        # 10K < total posts < 20K
        elif total_posts >= 10000 and total_posts < 20000:
            k100 = get_post_growth(sound, daily_change, one_day_growth_20k, '100K', buckets)

        # 100% chance of making it to 100K
        elif total_posts >= 100000:
            k100 = 1.0

        # total posts < 100K
        if total_posts < 100000:
            m1 = get_post_growth(sound, daily_change, one_day_growth_100k, '1M', buckets)

        # 100K < total posts < 250K
        elif total_posts >= 100000 and total_posts < 250000:
            m1 = get_post_growth(sound, daily_change, one_day_growth_250k, '1M', buckets)

        # 250K < total posts < 500K
        elif total_posts >= 250000 and total_posts < 500000:
            m1 = get_post_growth(sound, daily_change, one_day_growth_500k, '1M', buckets)

        # 100% chance of making it to 1M
        elif total_posts >= 1000000:
            m1 = 1.0

    return k100, m1


# FILTER 3 – 10K/100K POST MILESTONES
# Calculate the probability of sounds making it to 50K/100K/500K/1M posts based on their 10K/100K post milestones
def ten_k_post_milestones(sound):
    total_posts = sound['POST_COUNT']
    buckets = 'TOTAL POSTS'

    # Chances of making it to 50K posts
    k50 = 0.0
    # Chances of making it to 100K posts
    k100 = 0.0
    # Chances of making it to 500K posts
    k500 = 0.0
    # Chances of making it to 1M posts
    m1 = 0.0

    # 0 < total posts < 50K
    if total_posts >= 0 and total_posts < 50000:
        k50 = get_post_growth(sound, total_posts, post_milestones_10k, '50K', buckets)
        k100 = get_post_growth(sound, total_posts, post_milestones_10k, '100K', buckets)

    # 50K < total posts < 100K
    elif total_posts >= 50000 and total_posts < 100000:
        k50 = 1.0
        k100 = get_post_growth(sound, total_posts, post_milestones_10k, '100K', buckets)

    # total posts > 100K
    elif total_posts >= 100000:
        k50 = 1.0
        k100 = 1.0

    # 0 < total posts < 500K
    if total_posts >= 0 and total_posts < 500000:
        k500 = get_post_growth(sound, total_posts, post_milestones_100k, '500K', buckets)
        m1 = get_post_growth(sound, total_posts, post_milestones_100k, '1M', buckets)

    # 500K < total posts < 1M
    elif total_posts >= 500000 and total_posts < 1000000:
        k500 = 1.0
        m1 = get_post_growth(sound, total_posts, post_milestones_100k, '1M', buckets)

    # total posts > 1M
    elif total_posts >= 1000000:
        k500 = 1.0
        m1 = 1.0

    return k50, k100, k500, m1


# FILTER 4 – CONSISTENT 7-DAY GROWTH
# Calculate the probability of sounds reaching 50K/100K/500K/1M posts based on their growth over the last 7 days
def consistent_seven_day_growth(sound, timeseries):
    total_posts = sound['POST_COUNT']

    # Window size: 7 days
    window = 7

    # Only consider sounds with more than 10K posts
    if total_posts < 10000:

        # Retrieve the normalized timeseries data
        time_stamps, posts = get_timeseries(sound, timeseries, window=window)

        growth_scores = {}
        for index, post_bucket in consistent_growth_50k.iterrows():
            growth_scores[int(post_bucket['POSTS/DAY'])] = 0

        buckets = list(growth_scores.keys())

        for index in range(0, len(time_stamps) - 1):
            growth_period = time_stamps[index + 1] - time_stamps[index]

            if growth_period == 1:
                new_posts = posts[index + 1] - posts[index]
                growth_scores = get_post_bucket(buckets, new_posts, growth_scores)

        scores = list(growth_scores.values())

        chances_to_reach_50k, chances_to_reach_100k = [0.0], [0.0]

        for bucket, score in growth_scores.items():

            if score == 0:
                chances_to_reach_50k.append(0)
                chances_to_reach_100k.append(0)

            else:
                chances_to_reach_50k.append(consistent_growth_50k[str(score)][buckets.index(bucket)])
                chances_to_reach_100k.append(consistent_growth_100k[str(score)][buckets.index(bucket)])

        # Take the highest probability
        max_chance_50k, max_chance_100k = max(chances_to_reach_50k), max(chances_to_reach_100k)

        return max_chance_50k, max_chance_100k

    else:
        return 0.0, 0.0


# FILTER 5: 5-DAY ROLLING WINDOW AND GROWTH RATE
# Calculate the growth rate of a sound using a 5-day rolling window
def five_day_window(sound, timeseries):
    time_stamps, posts = get_timeseries(sound, timeseries)
    window_size = 5

    index, score = 0, 0

    while index < len(time_stamps) - 1:

        if (time_stamps[index + 1] - time_stamps[index]) == 1:
            score += 1

        else:
            break

        index += 1

    if score > 5:
        posts = posts[0:score - 1]

        return get_rolling_window(posts, window_size)

    # If there is not consistent data to build a 5-day window
    else:
        return 'Not sufficient data'


# HELPER METHODS

# Get the timestamps and number of posts on a particular window of data collection
# Retrieves the normalized timeseries data
def get_timeseries(sound, timeseries, window=None):
    sound_id = sound['TIKTOK_SOUND_SODATONE_ID']
    individual_timeseries = timeseries[timeseries.TIKTOK_SOUND_ID == sound_id]

    if window is not None:
        window = -abs(window)
        individual_timeseries = individual_timeseries.iloc[window:]

    time_stamps = individual_timeseries['CREATED_AT'].tolist()
    posts = individual_timeseries['POST_COUNT'].tolist()

    current_date = time_stamps[0].date()

    for index in range(0, len(time_stamps)):
        time_stamps[index] = abs((time_stamps[index].date() - current_date).days)

    return time_stamps, posts


# Calculate what bucket a particular number of posts falls into
def get_post_bucket(buckets, new_posts, growth_scores):
    for i in range(0, len(buckets)):

        if i == len(buckets) - 1:
            if new_posts >= buckets[i]:
                growth_scores[buckets[i]] += 1

        else:
            if new_posts >= buckets[i] and new_posts < buckets[i + 1]:
                growth_scores[buckets[i]] += 1

    return growth_scores


# Based on a sound's number of posts, retrieve its corresponding trajectory from Sodatone analysis
def get_post_growth(sound, posts, file, virality, buckets):
    growth_data_column = '% THAT REACH {}'.format(virality)

    for i in range(0, len(file)):

        if i == len(file) - 1:
            if posts >= file[buckets][i]:
                return file[growth_data_column][i]

        else:
            if posts >= file[buckets][i] and posts < file[buckets][i + 1]:
                return file[growth_data_column][i]


# Get the number of posts each day of a given window size
def get_rolling_window(series, window_size):
    moving_averages = []
    index = 0

    while index < len(series) - window_size + 1:
        current_window = series[index: index + window_size]

        window_average = sum(current_window) / window_size
        moving_averages.append(window_average)

        index += 1

    return moving_averages


# Calculate the average number of posts in a series of data
def average_window(moving_window):
    growth_rates = []

    for index in range(0, len(moving_window) - 1):
        growth_rate = (moving_window[index] - moving_window[index + 1]) / moving_window[index + 1]
        growth_rates.append(growth_rate)

    return growth_rates
