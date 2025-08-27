import pandas as pd
import os
from sklearn.model_selection import train_test_split
import logging

# creating a log directory
log_dir='logs'
os.makedirs(log_dir,exist_ok=True)

# logging configuration

# logger->handler{console handler,file handler}->formatter
# formatter is given passed in handlers and handler is passed in logger

# creating a logger 
logger=logging.getLogger('data_ingestion')
logger.setLevel('DEBUG')

# creating a file for filehandler
log_file_path=os.path.join(log_dir,'data_ingestion.log')
console_handler=logging.StreamHandler()
file_handler=logging.FileHandler(log_file_path)

# setting format 
formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# setting formatter in handlers
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# adding handlers in logger 
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# creating functions for data ingestion( for reading the data, and splitting the data, and for data cleaning)

def load_data(data_path:str)->pd.DataFrame:
    """Load data from csv file"""
    try:
        df=pd.read_csv(data_path)
        logger.debug("Data loaded from %s",data_path) #With f-strings, Python builds the full string before passing it to logger.debug With %s, Python only substitutes data_url if that log level is actually enabled.
        return df
    except pd.errors.ParserError as e:
        logger.error('Failed to parse the CSV file: %s', e)
        raise
    except Exception as e:
        logger.error('Unexpected error occurred while loading the data: %s', e)
        raise

def data_cleaning(df:pd.DataFrame)->pd.DataFrame:
    """Cleaning the data"""
    try:
        df.drop(columns = ['Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4'], inplace = True)
        df.rename(columns = {'v1': 'target', 'v2': 'text'}, inplace = True)
        logger.debug('Data preprocessing completed')
        return df
    except KeyError as e:
        logger.error('Missing column in the dataframe: %s', e)
        raise
    except Exception as e:
        logger.error('Unexpected error during preprocessing: %s', e)
        raise

def save_data(train_data:pd.DataFrame,test_data:pd.DataFrame,path_name:str)->None:
    try:
        raw_data_path = os.path.join(path_name, 'raw')
        os.makedirs(raw_data_path, exist_ok=True)
        train_data.to_csv(os.path.join(raw_data_path, "train.csv"), index=False)
        test_data.to_csv(os.path.join(raw_data_path, "test.csv"), index=False)
        logger.debug("Train and Test Data Successfully saved to %s",raw_data_path)
    except Exception as e:
        logger.error('Unexpected error occurred while saving the data: %s', e)
        raise
    
def main():
    try:
        data_path='./experiments/spam.csv'
        df=load_data(data_path)
        df=data_cleaning(df)
        train_data,test_data=train_test_split(df,test_size=0.2,random_state=2)
        save_data(train_data,test_data,'./data')
    except Exception as e:
        logger.error('Failed to complete the data ingestion process: %s', e)
        print(f"Error: {e}")



main()
