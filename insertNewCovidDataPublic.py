import pandas as pd
import datetime
from tqdm import tqdm

# Run the Python script to download the new data from the Kaggle API
import getCovidData

path = '/filepath/to/the/data'

# Import and format the new data in a Pandas dataframe
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
    return df

"""
This function computes the increase in confirmed cases, deaths, and recovered cases on a daily basis. 
The Kaggle dataset only lists the totals of each for each day and location, but not the daily increases and decreases.
The function also adds columns for the percentage increase/decrease each day.

This function can be run for each column

Note: This code is identical to the addAmountIncreaseCol function in generateCovidData.py
"""
def addAmountIncreaseCol(dataframe, columnName):

    # E.g. for confirmed cases, these column names become ConfirmedChange and ConfirmedPercentChange
    newCases = columnName + 'Change'
    newCasesPercent = columnName + 'PercentChange'
    dataframe[newCases] = 0
    dataframe[newCasesPercent] = 0.0
    dataframe[newCasesPercent].round(decimals = 2)
    
    # setting up the while loop
    # use day before yesterday and yesterday as first and second iteration because data always one day old
    currentDate = dayBeforeYesterday#dataframe['ObservationDate'].iloc[0]

    # use the second lastDate declaration for testing a smaller dataframe
    lastDate = yesterday
    howLong = (lastDate - dayBeforeYesterday).days
    pbar = tqdm(total= howLong,initial=1)


    """
    Loop through every date in the dataset starting in January, when the data begins
    Assign values for the daily changes and percent increase/decreases for each row
    """
    while currentDate <= lastDate:
        currDateConditional = dataframe['ObservationDate'] == currentDate

        previousDate = currentDate - datetime.timedelta(days = 1)
        prevDateConditional = dataframe['ObservationDate'] == previousDate
        
        allCurrStates = dataframe[dataframe['ObservationDate'] == currentDate]['Province/State'].tolist()
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
                        percentage = round(((difference / prevValue)*100),2)
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

# the dataset from kaggle will always have the name covid_19_data.csv
df = importData(path + 'covid_19_data.csv')

# Don't need these columns
df = df.drop(columns=['Last Update','SNo'])
 
# The data is always one day old since it's not live, so yesterday is always the latest date
today = datetime.date.today()
yesterday = today - datetime.timedelta(days = 1)
dayBeforeYesterday = yesterday - datetime.timedelta(days=1)

# Add the absolute and % increase/decrease columns for every row
# Only interested in the data from yesterday, since already have all other previous data
df = df[df['ObservationDate'] >= dayBeforeYesterday]
df = addAmountIncreaseCol(df,'Confirmed')
df = addAmountIncreaseCol(df,'Deaths')
df = addAmountIncreaseCol(df,'Recovered')

# Change the order of the columns so it's more intuitive and easier to work with
colNames = ['ObservationDate','Province/State','Country/Region','Confirmed','ConfirmedChange','ConfirmedPercentChange','Deaths', 'DeathsChange','DeathsPercentChange', 'Recovered', 'RecoveredChange', 'RecoveredPercentChange']
df = df.reindex(columns=colNames)
df = df[df['ObservationDate'] == yesterday]

df2 = importData(path + 'covidDataWithDifferences.csv')
df2 = df2.reindex(columns=colNames)

df2 = df2.append(df)

# Create a csv with up-to-date covid data and columns 
df2.to_csv(path + 'covidDataWithDifferences.csv')