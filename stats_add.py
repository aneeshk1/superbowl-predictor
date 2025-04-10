from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb+srv://aneesh:<db_password>@cluster65664.az03p.mongodb.net/"
DB_NAME = "Project-CSE-482"

def flatten_header(l1: str, l2: str):
    """ Flattens header for pandas to read from

    Various files have 2-tiered headers. Where:
    line 1[i] + line 2[i] = Col header for i
    Args:
        l1 = line 1 of header
        l2 = line 2 of header
    Returns:
        list of column headers after merging them
    """
    header_list = [item for item in l1.split(',')]

    l2 = list(l2.split(','))
    for i in range(len(header_list)):
        if header_list[i] == '':
            header_list[i] = header_list[i] + l2[i]
        else:
            header_list[i] = header_list[i] + " " + l2[i]

    return header_list


def make_df(file: str):
    """Make dataframes from csv files exported from ProFootball Ref

    Args:
        file: text file with csv formatted data
    Returns:
        Pandas DataFrame object with data from txt
    """
    with open(file, "r") as f:
        lines = f.readlines()
    df = pd.read_csv(file, names=flatten_header(lines[0], lines[1]))
    df = df.iloc[2:-3]    # drop first 2 rows and last 3 rows
    df = df.drop(["Rk", "G"], axis=1)    # drop rk and g columns as they are not needed
    return df


def add_stats(file: str, season: int):
    """Add stats from a txt file to the mongodb database

    Matches rows based on teams and updates accordingly
    Does AFC then NFC database according to the season number provided
    Args:
        file: Input txt file to read from 
        season: season for which to add stats to add(YY format)
    Returns:
        None
    """
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    AFC = db[f"AFC {season}"]
    NFC = db[f"NFC {season}"]

    # Convert DataFrame to a list of dictionaries (one per row)
    csv_data = make_df(file).to_dict("records")

    # For each row in CSV, update MongoDB if 'Tm' matches
    for row in csv_data:
        tm_value = row.get("Tm")
        update_data = {k: v for k, v in row.items()}

        # Update MongoDB document where 'Tm' matches for AFC teams
        result1 = AFC.update_one(
            {"Tm": tm_value},
            {"$set": update_data},
            upsert=False  # Do not insert if not found
        )
        # Update MongoDB document where 'Tm' matches for NFC teams
        result2 = NFC.update_one(
            {"Tm": tm_value},
            {"$set": update_data},
            upsert=False  # Do not insert if not found
        )


        # maintain a log
        if result1.matched_count > 0 or result2.matched_count > 0:
            pass
        else:
            print(f"ERROR: No match found for team: {tm_value}, {season}")

    print(f"Update completed for {season}")

def update_all_seasons(start, end):
    for season in range(start, end + 1):
        add_stats(f"off/{season}.txt", season)

