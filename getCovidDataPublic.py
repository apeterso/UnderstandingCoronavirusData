import kaggle

kaggle.api.authenticate()
kaggle.api.dataset_download_files('sudalairajkumar/novel-corona-virus-2019-dataset', path= '/filepath/to/the/data', unzip=True)