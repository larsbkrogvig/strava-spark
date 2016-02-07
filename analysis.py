from classes import StravaLoader

def main():

    df = StravaLoader('s3').get_dataset()

    df.show()
    df.printSchema()

    pass

if __name__ == '__main__':
    main()
