# Import required packages
import requests
from io import BytesIO
import matplotlib.pyplot as plt
import logging
import pandas as pd
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def fetch_data_from_api(url, file_name, limit=1000):
    """
    Function to pull data from an api endpoint, collecting data in chunks of 1000 rows and appending in a CSV file
    :param url: API endpoint
    :param file_name: desired file name of output
    :param limit: maximum number of records to pull in a chunk
    :return: None
    """
    offset = 0
    column_headers = True
    while True:
        # Set configs for HTTP request
        params = {"$limit": limit, "$offset": offset}
        logging.info(f"Fetching rows {offset + 1} to {offset + limit}.")

        try:
            with requests.get(url, params=params, stream=True) as response:
                response.raise_for_status()
                # Raw binary content of the response
                content = response.content
                chunk = pd.read_csv(BytesIO(content))  # Reading the in-memory chunk

            if chunk.empty:  # No more data
                logging.info("No more rows to fetch.")
                break

            # Write to CSV incrementally by using append mode
            chunk.to_csv(file_name, mode='a', index=False, header=column_headers)
            # Don't require header for the next chunk, or the final CSV will have header rows in between records
            column_headers = False
            # Move to the next batch
            offset += limit

        # Exception for Request Exception
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            break

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            break

    logging.info(f"Data written to {file_name}")


def preprocess_data(app_data):
    """
    Function to clean the data based on null values, calculating birth year from the applicant's DOB, the cleaned data
    is written as a CSV file
    :param app_data: Data pulled from API
    :return: None
    """
    # Records where any column contains a null
    inv_data = app_data[app_data.isnull().any(axis=1)]
    # Remove any null records from main dataframe
    app_data = app_data.dropna()

    # Make Senate column data in snake letters
    app_data['senate'] = app_data['senate'].str.replace(' ', '_').str.lower()
    # Calculate year of birth from DOB
    app_data['dateofbirth'] = pd.to_datetime(app_data['dateofbirth'], errors='coerce')
    app_data['yr_born'] = app_data['dateofbirth'].dt.year
    # Reorder columns to have "year born" next to DOB
    app_data = app_data[['countyname', 'party', 'dateofbirth', 'yr_born', 'mailapplicationtype',
                         'appissuedate', 'appreturndate', 'ballotsentdate', 'ballotreturneddate',
                         'legislative', 'senate', 'congressional']]
    # Write cleaned dataframe and invalid data frame
    app_data.to_csv("data/application_in_processed.csv", index=False, header=True)
    inv_data.to_csv("data/invalid_data.csv", index=False, header=True)
    logging.info(f"Preprocessed and Invalid Data written")


# API endpoint URL
API_URL = "https://data.pa.gov/resource/mcba-yywm.csv"
# Output filename
FILE_NAME = "data/application_in.csv"


def main():
    # Create "data" and "solution" folders for writing files
    if not os.path.exists('data'):
        os.makedirs('data')
    if not os.path.exists('solution'):
        os.makedirs('solution')

    # Fetch and save the data on csv
    fetch_data_from_api(API_URL, file_name="data/application_in.csv")
    # Read fetched data
    application_in = pd.read_csv(FILE_NAME)
    # Preprocess data
    preprocess_data(application_in)
    # Read preprocessed data
    application_proc = pd.read_csv("data/application_in_processed.csv")

    ## Question 1: How does applicant age (in years) and party designation (party) relate to overall vote
    # by mail requests?
    data_age_analysis = application_proc[application_proc['yr_born'] != 1800]
    # Calculate age according to year 2020
    data_age_analysis['age'] = 2020 - data_age_analysis['yr_born']

    # Create age groups
    bins = [0, 25, 35, 45, 55, 65, 140]
    labels = ['under 25', '26-35', '36-45', '46-55', '56-65', 'over 65']
    data_age_analysis['age_group'] = pd.cut(data_age_analysis['age'], bins=bins, labels=labels, right=False)
    # Group on Party and Age Group, counting the number of mail requests
    vote_by_mail_stats = data_age_analysis.groupby(['party', 'age_group']).size().reset_index(name='count')
    # Order the records based on party and number of mail requests
    vote_by_mail_stats = vote_by_mail_stats.sort_values(by=['party', 'count'], ascending=[False, False])
    # Write the solution data to respective folder
    vote_by_mail_stats.to_csv('solution/1_AgePartyAnalysis.csv', index=False, header=True)
    logging.info(f"Question 1 solved")

    ## Question 2: What was the median latency from when each legislative district (legislative) issued their
    # application and when the ballot was returned?
    # Calculate latency in days
    # Change to appropriate datatype
    application_proc['appissuedate'] = pd.to_datetime(application_proc['appissuedate'], errors='coerce')
    application_proc['appreturndate'] = pd.to_datetime(application_proc['appreturndate'], errors='coerce')
    application_proc['ballotsentdate'] = pd.to_datetime(application_proc['ballotsentdate'], errors='coerce')
    application_proc['ballotreturneddate'] = pd.to_datetime(application_proc['ballotreturneddate'], errors='coerce')
    # Calculate latency days from the day of application issue till the day of ballot recieved at office
    application_proc['latency_days'] = (application_proc['ballotreturneddate'] - application_proc['appissuedate']).dt.days

    # Group by legislative district and calculate the median latency
    median_latency = application_proc.groupby('legislative')['latency_days'].median().reset_index() \
        .rename(columns={'latency_days': 'median_latency_days'})

    # Sort by median latency for clarity
    median_latency = median_latency.sort_values(by='median_latency_days', ascending=False)
    # Write the solution data to respective folder
    median_latency.to_csv('solution/2_MedianLatencyLegislative.csv', index=False, header=True)

    logging.info(f"Question 2 solved")

    ## Question 3: What is the congressional district (congressional) that has the highest frequency of ballot requests?
    # Group by congressional district and count the occurrences
    congressional_counts = application_proc.groupby('congressional').size().reset_index(name='request_count')\
        .sort_values(by='request_count', ascending=False)

    # Get the district with the highest frequency
    top_congressional_district = congressional_counts.head(1)
    # Write the solution data to respective folder
    top_congressional_district.to_csv('solution/3_TopCongressionalDistrict.csv', index=False, header=True)

    logging.info(f"Question 3 solved")

    ## Question 4: Create a visualization demonstrating the republican and democratic application counts in each county.
    # Filter for relevant parties
    data_rep_dem = application_proc[application_proc['party'].isin(['R', 'D'])]

    # Group by county and party and aggregating on the count the applications
    county_party_counts = data_rep_dem.groupby(['countyname', 'party']).size().unstack(fill_value=0).reset_index()\
        .rename(columns={'R': 'Republican', 'D': 'Democrat'})

    # Create stacked bar chart
    county_party_counts.set_index('countyname').plot(kind='bar', stacked=True, figsize=(12, 6), color=['red', 'blue'],
                                                     title='Republican vs Democratic Party Applications by County')
    # Config for visualization
    plt.xlabel('County')
    plt.ylabel('Total Applications')
    plt.legend(title='Party')
    plt.tight_layout()
    # Write the solution data to respective folder
    plt.savefig('solution/4_PartyCounty.png', dpi=300)
    logging.info(f"Question 4 solved")


if __name__ == '__main__':
    main()
