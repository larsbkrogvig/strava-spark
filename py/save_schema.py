import pickle
from classes import StravaLoader

def main():

    df = StravaLoader('local', 'strava-activities-subset').get_dataset()
    df.printSchema()
    
    pickle.dump(df.schema, open("schema.p", "wb"))

    pass

if __name__ == '__main__':
    main()
