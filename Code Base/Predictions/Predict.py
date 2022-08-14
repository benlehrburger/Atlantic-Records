# MAKE PREDICTIONS

# Apply the 5 filters to the aggregate dataframe
# Add new columns to hold probabilities of making it to 50K/100K/500K/1M posts
def add_aggregate_metrics(mutable_df, timeseries):
    for index, sound in mutable_df.iterrows():

        filter1_50k, filter1_100k, filter1_500k, filter1_1m = already_viral(sound)
        filter2_100k, filter2_1m = large_one_day_post_growth(sound)
        filter3_50k, filter3_100k, filter3_500k, filter3_1m = ten_k_post_milestones(sound)
        filter4_50k, filter4_100k = consistent_seven_day_growth(sound, timeseries)

        chances_of_reaching_50k = [filter1_50k, filter3_50k, filter4_50k]
        chances_of_reaching_100k = [filter1_100k, filter2_100k, filter3_100k, filter4_100k]
        chances_of_reaching_500k = [filter1_500k, filter3_500k]
        chances_of_reaching_1m = [filter1_1m, filter2_1m, filter3_1m]

        max_chance_50k = max(chances_of_reaching_50k)
        max_chance_100k = max(chances_of_reaching_100k)
        max_chance_500k = max(chances_of_reaching_500k)
        max_chance_1m = max(chances_of_reaching_1m)

        mutable_df.at[index, 'CHANCES_OF_REACHING_50K'] = round(max_chance_50k, 3)
        mutable_df.at[index, 'CHANCES_OF_REACHING_100K'] = round(max_chance_100k, 3)
        mutable_df.at[index, 'CHANCES_OF_REACHING_500K'] = round(max_chance_500k, 3)
        mutable_df.at[index, 'CHANCES_OF_REACHING_1M'] = round(max_chance_1m, 3)

        window = five_day_window(sound, timeseries)

        if window != 'Not sufficient data' and len(window) > 1:

            growth = average_window(window)
            mutable_df.at[index, '5_DAY_ROLLING_WINDOW'] = window
            mutable_df.at[index, '5_DAY_GROWTH_RATE'] = growth[0]

        else:

            mutable_df.at[index, '5_DAY_ROLLING_WINDOW'] = 0.0
            mutable_df.at[index, '5_DAY_GROWTH_RATE'] = 0.0

    return mutable_df