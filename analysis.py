from classes import StravaLoader

def main():

    df = StravaLoader('local').get_dataset()

    df.show()
    df.printSchema()

    pass

if __name__ == '__main__':
    main()
