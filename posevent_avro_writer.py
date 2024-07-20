import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

def main():

    try:
        schemafile = "avro/posevent.avsc"
        schema = avro.schema.parse(open(schemafile, "rb").read())
    except (FileNotFoundError, OSError):
        print(f"Schema file {schemafile} not found")

    try:
        avrobinfile = "posevent.avro"
        writer = DataFileWriter(open(avrobinfile, "wb"), DatumWriter(), schema)
        writer.append({
            "store_id": 1234,
            "workstation_id": 677,
            "operator_id": "AB4567",
            "item_id": 2345,
            "item_desc": "Pelican basmati rice, 500 gms",
            "unit_price": 24.50,
            "txn_date": 1721397292000
        })
        writer.close()
    except OSError:
        print(f"Avro binary file {avrobinfile} could not be written")

if __name__ == '__main__':
    main()
