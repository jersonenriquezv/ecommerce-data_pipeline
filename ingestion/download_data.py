import kaggle

def download_data():
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('gabrielramos87/an-online-shop-business', unzip=True)

download_data()
