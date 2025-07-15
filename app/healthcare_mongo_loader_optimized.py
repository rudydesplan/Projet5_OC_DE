import pandas as pd
from pymongo import MongoClient, InsertOne
from pymongo.write_concern import WriteConcern
from bson.codec_options import CodecOptions, DatetimeConversion
import pendulum
from loguru import logger
import argparse

# --- Logging Setup ---
logger.add("mongo_loader.log", rotation="500 KB", retention="5 days", level="INFO")

# --- BSON CodecOptions for timezone-aware UTC datetimes ---
codec_opts = CodecOptions(
    tz_aware=True,
    tzinfo=pendulum.UTC,
    datetime_conversion=DatetimeConversion.DATETIME_AUTO
)

# JSON Schema for "Patients" collection (identical à la version précédente)
patient_schema = {
    "bsonType": "object",
    "required": ["Name", "Age", "Gender", "Blood Type", "Medical Condition",
                 "Date of Admission", "Doctor", "Hospital", "Billing Amount",
                 "Admission Type", "Discharge Date", "Medication", "Test Results"],
    "properties": {
        "Name": {"bsonType": "string"},
        "Age": {"bsonType": "int", "minimum": 0},
        "Gender": {"enum": ["Male", "Female"]},
        "Blood Type": {"enum": ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]},
        "Medical Condition": {"enum": ["Cancer", "Obesity", "Diabetes", "Asthma", "Hypertension", "None"]},
        "Date of Admission": {"bsonType": "date"},
        "Doctor": {"bsonType": "string"},
        "Hospital": {"bsonType": "string"},
        "Insurance Provider": {"bsonType": "string"},
        "Billing Amount": {"bsonType": "double"},
        "Room Number": {"bsonType": "int"},
        "Admission Type": {"enum": ["Urgent", "Emergency", "Elective"]},
        "Discharge Date": {"bsonType": "date"},
        "Medication": {"bsonType": "string"},
        "Test Results": {"enum": ["Normal", "Abnormal", "Inconclusive"]}
    }
}

def get_collection(client, db_name="HealthcareDB", coll_name="Patients"):
    db = client.get_database(db_name, codec_options=codec_opts)
    return db.get_collection(coll_name, write_concern=WriteConcern(w=1))

def create_schema(collection):
    logger.info("📘 Création/mise à jour du schéma pour 'Patients'")
    db = collection.database
    coll_name = collection.name

    if coll_name in db.list_collection_names():
        # Collection existe : on modifie le validateur
        try:
            db.command({
                "collMod": coll_name,
                "validator": {"$jsonSchema": patient_schema},
                "validationLevel": "strict",  # ou "moderate"
                "validationAction": "error"
            })
            logger.success("✅ Schéma mis à jour via collMod.")
        except Exception as e:
            logger.error(f"❌ Erreur lors de collMod : {e}")
    else:
        # Si elle n'existe pas encore
        try:
            db.create_collection(
                coll_name,
                validator={"$jsonSchema": patient_schema},
                validationAction="error"
            )
            logger.success("✅ Collection créée avec schéma.")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de la collection : {e}")

def create_indexes(collection):
    logger.info("⚙️ Création des indexes...")
    indexes = [
        (["Name", "Date of Admission"], {}),  # plus de `unique=True` ici
        (["Date of Admission"], {}),
        (["Discharge Date"], {}),
        (["Medical Condition"], {}),
        (["Hospital", "Admission Type"], {}),
        (["Doctor"], {})
    ]
    for fields, opts in indexes:
        try:
            name = collection.create_index([(f, 1) for f in fields], **opts)
            logger.info(f"✅ Index créé : {name}")
        except Exception as e:
            logger.error(f"❌ Erreur création index {fields}: {e}")


def load_data(csv_path, collection, batch_size=2000):
    logger.info(f"Loading CSV from {csv_path}")
    df = pd.read_csv(csv_path)

    # Dates -> pendulum-aware
    df["Date of Admission"] = pd.to_datetime(df["Date of Admission"])
    df["Discharge Date"] = pd.to_datetime(df["Discharge Date"])

    # Standardisation texte
    text_cols = ["Name","Doctor","Hospital","Insurance Provider",
                 "Medication","Test Results","Gender","Blood Type",
                 "Medical Condition","Admission Type"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.title()
    df["Blood Type"] = df["Blood Type"].str.upper()
    df["Gender"] = df["Gender"].str.capitalize()

    def conv_row(r):
        try:
            return {
                "Name": r["Name"],
                "Age": int(r["Age"]),
                "Gender": r["Gender"],
                "Blood Type": r["Blood Type"],
                "Medical Condition": r["Medical Condition"],
                "Date of Admission": pendulum.parse(str(r["Date of Admission"])).naive(),
                "Doctor": r["Doctor"],
                "Hospital": r["Hospital"],
                "Insurance Provider": r["Insurance Provider"],
                "Billing Amount": float(r["Billing Amount"]),
                "Room Number": int(r["Room Number"]),
                "Admission Type": r["Admission Type"],
                "Discharge Date": pendulum.parse(str(r["Discharge Date"])).naive(),
                "Medication": r["Medication"],
                "Test Results": r["Test Results"]
            }
        except Exception as e:
            logger.error(f"❌ Conversion error in row {r.name}: {e}")
            return None

    total = len(df)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        docs = [conv_row(r) for _, r in df.iloc[start:end].iterrows()]
        docs = [d for d in docs if d]

        if docs:
            try:
                collection.bulk_write(
                    [InsertOne(d) for d in docs],
                    ordered=False
                )
                logger.success(f"📥 Batch {start}-{end}: {len(docs)} documents inserted.")
            except Exception as e:
                logger.error(f"❌ Bulk write error in batch {start}-{end}: {e}")
        else:
            logger.warning(f"⚠️ Batch {start}-{end} contains no valid docs, skipped.")

    logger.info("🎉 Data load complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, default="./data/healthcare_dataset.csv", help="Path to input CSV file")
    args = parser.parse_args()

    logger.info("🔗 Connecting to MongoDB...")
    client = MongoClient("mongodb://localhost:27017", tz_aware=True)
    coll = get_collection(client)

    create_schema(coll)
    create_indexes(coll)
    load_data(args.csv, coll)