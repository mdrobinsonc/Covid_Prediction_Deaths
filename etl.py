# Importing the necessary libraries:
# - pandas: for data manipulation and analysis
# - os: for path file and directory handling
# - argparse: for parsing command-line arguments
# - datetime & timedelta: for handling date and time operations

import pandas as pd
import os
import argparse
from datetime import datetime, timedelta


def get_parameters_args():
    """
    Handles command-line arguments for configuring the ETL pipeline.
    """
    argument_parser = argparse.ArgumentParser(description="Pipeline for processing and generating the macrotable.")
    argument_parser.add_argument('input', type=str, help="Path to the folder containing the input datasets.")
    argument_parser.add_argument('-o', '--output', type=str, default="macrotable.csv", help="File path to save the resulting macrotable.")
    argument_parser.add_argument('--start', type=str, default="2020-01-02", help="Start date for filtering data (inclusive).")
    argument_parser.add_argument('--end', type=str, default="2022-08-22", help="End date for filtering data (inclusive).")
    argument_parser.add_argument('--countries', type=str, nargs='*', default=None, help="List of countries to include in the analysis.")
    return argument_parser.parse_args()

def import_data(directory):
    """
    Import data from CSV files located in the specified directory.
    """
    # Reading demographics data
    demographics_data = pd.read_csv(os.path.join(directory, 'demographics.csv'))
    # Reading epidemiology data
    epidemiology_data = pd.read_csv(os.path.join(directory, 'epidemiology.csv'))
    # Reading health-related data
    health_data = pd.read_csv(os.path.join(directory, 'health.csv'))
    # Reading hospitalizations data
    hospitalizations_data = pd.read_csv(os.path.join(directory, 'hospitalizations.csv'))
    # Reading index-related data
    index_data = pd.read_csv(os.path.join(directory, 'index.csv'))
    # Reading vaccinations data
    vaccinations_data = pd.read_csv(os.path.join(directory, 'vaccinations.csv'))
    # Returning all loaded datasets
    return demographics_data, epidemiology_data, health_data, hospitalizations_data, index_data, vaccinations_data


def combine_data(demographics_data, epidemiology_data, health_data, hospitalizations_data, index_data, vaccinations_data):  
    """
    Merge all input datasets into a single DataFrame for further processing.
    """
    # Merge demographics and epidemiology datasets on 'location_key' using a left join
    macrotable = pd.merge(demographics_data, epidemiology_data, on=['location_key'], how='left')
    # Add health data to the macrotable DataFrame using a left join on 'location_key'
    macrotable = pd.merge(macrotable, health_data, on=['location_key'], how='left')
    # Merge hospitalizations data, matching on 'location_key' and 'date'
    macrotable = pd.merge(macrotable, hospitalizations_data, on=['location_key', 'date'], how='left')
    # Add index data to the macrotable DataFrame using a left join on 'location_key'
    macrotable = pd.merge(macrotable, index_data, on=['location_key'], how='left')
    # Merge vaccination data, matching on 'location_key' and 'date'
    macrotable = pd.merge(macrotable, vaccinations_data, on=['location_key', 'date'], how='left')


    # Establish an 60% data availability threshold for retaining columns (removing the columns that have more than 60% nulls)
    min_data_threshold = len(macrotable) * 0.6
    # Remove columns above our threshold
    macrotable = macrotable.dropna(thresh=min_data_threshold, axis=1)


    # Remove non-essential columns that do not contribute to the analysis of our project
    columns = ['datacommons_id', 'place_id', 'subregion2_code', 'subregion2_name', 'wikidata_id', 'subregion1_name', 'iso_3166_1_alpha_3', 'aggregation_level', 'subregion1_code', 'iso_3166_1_alpha_2', 'life_expectancy']
    macrotable = macrotable.drop(columns=columns)

        # Fill in missing values for population-related columns using the average (mean) to ensure data completeness
    macrotable = macrotable.fillna(
    {'population_age_20_29': macrotable.population_age_20_29.mean(),
     'population_age_70_79': macrotable.population_age_70_79.mean(),
     'population_age_60_69': macrotable.population_age_60_69.mean(),
     'population_age_50_59': macrotable.population_age_50_59.mean(),
     'population_age_40_49': macrotable.population_age_40_49.mean(),
     'population_age_30_39': macrotable.population_age_30_39.mean(),
     'population_age_80_and_older': macrotable.population_age_80_and_older.mean(),
     'population_age_10_19': macrotable.population_age_10_19.mean(),
     'population_age_00_09': macrotable.population_age_00_09.mean(),
     'population_female': macrotable.population_female.mean(),
     'population_male': macrotable.population_male.mean()
     })
    
    # Fill missing values in the 'population' column by adding values from 'population_male' and 'population_female'
    macrotable['population'] = macrotable['population'].fillna(macrotable['population_male'] + macrotable['population_female'])

    # Sort the dataset by 'location_key' and 'date' to maintain chronological order within each location
    macrotable.sort_values(by=['location_key', 'date'], inplace=True)

    # Handle missing and invalid values in the 'new_confirmed' column
    macrotable['new_confirmed'] = macrotable['new_confirmed'].fillna(0)  # Replace null values with 0
    macrotable.loc[macrotable['new_confirmed'] < 0, 'new_confirmed'] = 0  # Replace negative values with 0

    # Handle missing and invalid values in the 'new_deceased' column
    macrotable['new_deceased'] = macrotable['new_deceased'].fillna(0)  # Replace null values with 0
    macrotable.loc[macrotable['new_deceased'] < 0, 'new_deceased'] = 0  # Replace negative values with 0

    # Compute cumulative values for 'new_confirmed' and 'new_deceased' columns grouped by 'location_key'
    macrotable['cumulative_confirmed'] = macrotable.groupby('location_key')['new_confirmed'].cumsum()
    macrotable['cumulative_deceased'] = macrotable.groupby('location_key')['new_deceased'].cumsum()
    
    # Remove rows where critical columns have missing values, as they are essential for analysis
    macrotable = macrotable.dropna(subset=['country_code'])
    macrotable = macrotable.dropna(subset=['date'])  
    macrotable = macrotable.dropna(subset=['cumulative_confirmed'])  # Drop rows missing 'cumulative_confirmed'

    # Convert the 'date' column from string format to datetime for consistency in date operations
    macrotable['date'] = pd.to_datetime(macrotable['date'])

    return macrotable

def refine_data(macrotable, start_period, end_period, country_list):
    """
    Apply filtering to the dataset based on the specified date range and list of countries.
    """
    # Filter rows that fall within the specified date range
    date_filter = (macrotable['date'] >= start_period) & (macrotable['date'] <= end_period)
    macrotable = macrotable[date_filter]
    
    # Further filter the data to include only the specified countries, if provided
    if country_list:
        macrotable = macrotable[macrotable['country_name'].isin(country_list)]
    
    return macrotable

def aggregate_data(macrotable, epidemiology_data, demographics_data, index_data):

    def create_date_intervals(start_date, end_date, interval_days):
        """
        Generate date ranges with a specified interval.
        """
        date_intervals = []
        interval_start = datetime.strptime(start_date, "%Y-%m-%d")
        interval_end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Loop through and create intervals until the end date
        while interval_start <= interval_end_date:
            interval_end = interval_start + timedelta(days=interval_days - 1)
            if interval_end > interval_end_date:
                interval_end = interval_end_date  # Ensure the last interval doesn't exceed the end date
            date_intervals.append(f"{interval_start.strftime('%Y-%m-%d')}/{interval_end.strftime('%Y-%m-%d')}")
            interval_start = interval_end + timedelta(days=1)

        return date_intervals
    
    
    # Determine the earliest and latest dates in the epidemiology dataset
    min_date = epidemiology_data.date.min()
    max_date = epidemiology_data.date.max()

    # Generate weekly date ranges within the identified date range
    date_ranges = create_date_intervals(min_date, max_date, 7)
    
    # Create a mapping dictionary to associate each date with its respective week range
    date_ranges_dict = {}
    for date_range in date_ranges:
        start, end = date_range.split('/')
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")
        current_date = start_date
        while current_date <= end_date:
            date_ranges_dict[current_date] = date_range
            current_date += timedelta(days=1)

    # Assign each date in the macrotable to its corresponding weekly range
    macrotable['week'] = [date_ranges_dict[date] for date in macrotable['date']]

       # Aggregate data weekly by 'week' and 'country_name', summing the key metrics 'new_confirmed' and 'new_deceased'
    aggregated_df = macrotable.groupby(['week', 'country_name']).agg({
        'new_confirmed': 'sum',  # Total confirmed cases for the week
        'new_deceased': 'sum',  # Total deceased cases for the week
    }).sort_values(['country_name', 'week']).reset_index()

    # Create a cumulative sum column for confirmed cases ('cumulative_confirmed') by country
    for i in range(len(aggregated_df)):
        # If it's the first entry for the country, initialize cumulative cases to the weekly total
        if i == 0 or aggregated_df.loc[i, 'country_name'] != aggregated_df.loc[i-1, 'country_name']:
            aggregated_df.loc[i, 'cumulative_confirmed'] = aggregated_df.loc[i, 'new_confirmed']
        else:
            # Otherwise, add the current week's cases to the cumulative total
            aggregated_df.loc[i, 'cumulative_confirmed'] = aggregated_df.loc[i-1, 'cumulative_confirmed'] + aggregated_df.loc[i, 'new_confirmed']

    # Create a cumulative sum column for deceased cases ('cumulative_deceased') by country
    for i in range(len(aggregated_df)):
        # If it's the first entry for the country, initialize cumulative deaths to the weekly total
        if i == 0 or aggregated_df.loc[i, 'country_name'] != aggregated_df.loc[i-1, 'country_name']:
            aggregated_df.loc[i, 'cumulative_deceased'] = aggregated_df.loc[i, 'new_deceased']
        else:
            # Otherwise, add the current week's deaths to the cumulative total
            aggregated_df.loc[i, 'cumulative_deceased'] = aggregated_df.loc[i-1, 'cumulative_deceased'] + aggregated_df.loc[i, 'new_deceased']

    # Ensure the aggregated DataFrame is sorted by 'week' for chronological analysis
    aggregated_df = aggregated_df.sort_values('week')
    
    # Merge demographic and index datasets to calculate population statistics for each country
    demographics_index = pd.merge(demographics_data, index_data, on='location_key', how='left')

    # Summarize population statistics for each country by aggregating demographic data
    population_by_country = demographics_index.groupby('country_name').agg({
        'population': 'sum',  # Total population across all locations in the country
        'population_male': 'sum',  # Total male population
        'population_female': 'sum',  # Total female population
        'population_age_00_09': 'sum',  # Total population aged 0-9
        'population_age_10_19': 'sum',  # Total population aged 10-19
        'population_age_20_29': 'sum',  # Total population aged 20-29
        'population_age_30_39': 'sum',  # Total population aged 30-39
        'population_age_70_79': 'sum',  # Total population aged 70-79
        'population_age_60_69': 'sum',  # Total population aged 60-69
        'population_age_50_59': 'sum',  # Total population aged 50-59
        'population_age_40_49': 'sum',  # Total population aged 40-49
        'population_age_80_and_older': 'sum'  # Total population aged 80 and older
    }).reset_index()

    # Merge the aggregated population data with the aggregated DataFrame by country
    macrotable = pd.merge(aggregated_df, population_by_country, on='country_name', how='left')

    return macrotable


def main():
    """
    Main function to orchestrate the ETL process: loading, processing, filtering, and saving data.
    """
    args = get_parameters_args() # Parse command-line arguments
    
    # Load all required datasets from the specified directory
    demographics_data, epidemiology_data, health_data, hospitalizations_data, index_data, vaccinations_data = import_data(args.input)
    
    # Combine and preprocess all datasets into a unified macrotable
    macrotable = combine_data(demographics_data, epidemiology_data, health_data, hospitalizations_data, index_data, vaccinations_data)

    # Filter the macrotable based on the user-specified date range and countries
    macrotable = refine_data(macrotable, args.start, args.end, args.countries)
    
    # Aggregate data weekly by country and include population details
    macrotable = aggregate_data(macrotable, epidemiology_data, demographics_data, index_data)

    # Set 'week' and 'country_name' as the DataFrame's index for easier analysis
    macrotable.set_index(['week', 'country_name'], inplace=True)

    # Save the resulting macrotable as a CSV file to the user-specified output location
    macrotable.to_csv(args.output, index=True)
    print(f"Macrotable saved to {args.output}")


if __name__ == "__main__":
    main()