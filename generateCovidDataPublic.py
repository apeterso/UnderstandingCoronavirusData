import pandas as pd
import kaggle
import datetime
from tqdm import tqdm

# Setup the Kaggle API
kaggle.api.authenticate()

path = #/filepath/to/the/data

# Download the coronavirus data from a popular Kaggle dataset that is updated daily
kaggle.api.dataset_download_files('sudalairajkumar/novel-corona-virus-2019-dataset', path=path, unzip=True)


# Import and format the data in a Pandas dataframe
def importData(datasetFile):
    df = pd.read_csv(datasetFile)

    # Make all of the case figures into integers since it's cleaner and there can't be fractions
    df['Confirmed'] = pd.to_numeric(df['Confirmed'], downcast='integer')
    df['Deaths'] = pd.to_numeric(df['Deaths'], downcast='integer')
    df['Recovered'] = pd.to_numeric(df['Recovered'], downcast='integer')

    # Working with a date object is easier than a string for the purpose of this code
    df['ObservationDate'] = pd.to_datetime(df['ObservationDate'])
    df['ObservationDate'] = df['ObservationDate'].dt.date

    df['Province/State'] = df['Province/State'].astype(str)
    df['Country/Region'] = df['Country/Region'].astype(str)

    # Don't need these columns
    df = df.drop(columns=['SNo','Last Update'])
    df.fillna('-')
    return df

"""
This function computes the increase in confirmed cases, deaths, and recovered cases on a daily basis. 
The Kaggle dataset only lists the totals of each for each day and location, but not the daily increases and decreases.
The function also adds columns for the percentage increase/decrease each day.

This function can be run for each column
"""
def addAmountIncreaseCol(dataframe, columnName):

    # E.g. for confirmed cases, these column names become ConfirmedChange and ConfirmedPercentChange
    newCases = columnName + 'Change'
    newCasesPercent = columnName + 'PercentChange'
    dataframe[newCases] = 0
    dataframe[newCasesPercent] = 0.00
    dataframe[newCasesPercent].round(decimals = 2)
    currentDate = datetime.date(2020,1,23)

    # use the second lastDate declaration for testing a smaller dataframe
    lastDate = dataframe['ObservationDate'].iloc[-1]
    #lastDate = datetime.date(2020,1,26)

    howLong = (lastDate - dataframe['ObservationDate'].iloc[0]).days
    pbar = tqdm(total= howLong,initial=1)

    """
    Loop through every date in the dataset starting in January, when the data begins
    Assign values for the daily changes and percent increase/decreases for each row
    """
    while currentDate <= lastDate:
        currDateConditional = dataframe['ObservationDate'] == currentDate
        previousDate = currentDate - datetime.timedelta(days = 1)
        prevDateConditional = dataframe['ObservationDate'] == previousDate
        
        allCurrStates = dataframe[currDateConditional]['Province/State'].tolist()
        withoutStatesConditional = dataframe['Province/State'] == 'nan'
        allCurrCountries = dataframe[currDateConditional & withoutStatesConditional]['Country/Region'].tolist()
        
        allPrevStates = dataframe[prevDateConditional]['Province/State'].tolist()
        allPrevCountries = dataframe[prevDateConditional & withoutStatesConditional]['Country/Region'].tolist()

        # Some countries have state-leve data, so need to check if the state/province column equals null
        # New countries and states can be added each day, so need to have conditionals for new states and countries
        for state in allCurrStates:
            if state != 'nan':
                if state in allPrevStates:
                    stateConditional = dataframe['Province/State'] == state
                    prevValue = dataframe[prevDateConditional & stateConditional][columnName].iloc[0]
                    currValue = dataframe[currDateConditional & stateConditional][columnName].iloc[0]
                    difference = currValue - prevValue
                    if prevValue == 0:
                        percentage = 0.0
                    else:
                        percentage = round((difference / prevValue)*100,2)
                    dataframe.loc[currDateConditional & stateConditional, newCasesPercent] = percentage
                    dataframe.loc[currDateConditional & stateConditional, newCases] = difference
            if state == 'nan':
                for country in allCurrCountries:
                    
                    if country in allPrevCountries:
                        if state == 'nan':
                            countryConditional = dataframe['Country/Region'] == country
                            prevValue = dataframe[prevDateConditional & countryConditional][columnName].iloc[0]
                            currValue = dataframe[currDateConditional & countryConditional][columnName].iloc[0]
                            difference = currValue - prevValue
                            if prevValue == 0:
                                percentage = 0.0
                            else:
                                percentage = round(((difference / prevValue)*100),2)
                            dataframe.loc[currDateConditional & countryConditional, newCasesPercent] = percentage
                            dataframe.loc[currDateConditional & countryConditional,newCases] = difference
        pbar.update(1)
        currentDate = currentDate + datetime.timedelta(days = 1)
    pbar.close()
    return dataframe

# The dataset from kaggle will always have the name covid_19_data.csv
df = importData(path + 'covid_19_data.csv')
 
today = datetime.date.today()
yesterday = today - datetime.timedelta(days = 1)

# Add the absolute and % increase/decrease columns for every row
df = addAmountIncreaseCol(df,'Confirmed')
df = addAmountIncreaseCol(df,'Deaths')
df = addAmountIncreaseCol(df,'Recovered')

# Change the order of the columns so it's more intuitive
colNames = ['ObservationDate','Province/State','Country/Region','Confirmed','ConfirmedChange','ConfirmedPercentChange','Deaths', 'DeathsChange','DeathsPercentChange', 'Recovered', 'RecoveredChange', 'RecoveredPercentChange']
df = df.reindex(columns=colNames)

# Create a csv with up-to-date covid data and columns 
df.to_csv(path + 'covidDataWithDifferences.csv')