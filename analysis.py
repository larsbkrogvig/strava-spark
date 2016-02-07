from classes import StravaLoader

def main():

    df = StravaLoader('s3').get_dataset()
    df.printSchema()

    counts = df.groupBy(['athlete', 'activity_type']).count().orderBy(['athlete', 'activity_type'])
    counts.show()

    pass

if __name__ == '__main__':
    main()
