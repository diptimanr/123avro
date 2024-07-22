from avro.datafile import DataFileReader
from avro.io import DatumReader

def main():
    try:
        avrobinfile = "posevent.avro"
        reader = DataFileReader(open(avrobinfile, "rb"), DatumReader())
        for sample in reader:
            print(sample)

        reader.close()
    except FileNotFoundError:
        print(f"{avrobinfile} not found.")

if __name__ == '__main__':
    main()
