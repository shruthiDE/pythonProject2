

def loadData(transform,filepath="afterTransform.csv"):
    if transform is not None:
        try:
            transform.to_csv(filepath,index=False)
            print(f"Data loaded successfully to {filepath}")

        except Exception as e:
            print(f"Error loading data into csv file:{e}")

    else:
        print("No data to load")