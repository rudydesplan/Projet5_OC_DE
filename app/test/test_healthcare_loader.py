#test_healthcare_loader.py

import pytest
import mongomock
import pandas as pd
import pendulum
from pathlib import Path
from bson.codec_options import CodecOptions, DatetimeConversion
from pymongo.errors import OperationFailure
from healthcare_mongo_loader_optimized import (
    create_schema,
    load_data,
    get_collection,
    patient_schema
)

@pytest.fixture
def mock_mongo_client(monkeypatch):
    client = mongomock.MongoClient()
    monkeypatch.setattr("healthcare_mongo_loader_optimized.MongoClient", lambda *a, **k: client)
    return client

@pytest.fixture
def test_csv(tmp_path: Path):
    data = {
        "Name": ["John Doe", "Jane Smith"],
        "Age": [35, 42],
        "Gender": ["male", "female"],
        "Blood Type": ["A+", "B-"],
        "Medical Condition": ["Diabetes", "Hypertension"],
        "Date of Admission": ["2023-01-15", "2023-02-20"],
        "Doctor": ["Dr. Brown", "Dr. Taylor"],
        "Hospital": ["City General", "Metro Hospital"],
        "Insurance Provider": ["HealthPlus", "MediCare"],
        "Billing Amount": [2500.75, 3200.50],
        "Room Number": [305, 412],
        "Admission Type": ["Urgent", "Elective"],
        "Discharge Date": ["2023-01-22", "2023-03-01"],
        "Medication": ["Insulin", "Lisinopril"],
        "Test Results": ["Normal", "Abnormal"]
    }
    df = pd.DataFrame(data)
    file_path = tmp_path / "patients.csv"
    df.to_csv(file_path, index=False)
    return file_path

def test_create_schema(mock_mongo_client):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)
    assert collection.name in collection.database.list_collection_names()

def test_load_data_transformation(mock_mongo_client, test_csv):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)
    load_data(str(test_csv), collection, batch_size=1)

    assert collection.count_documents({}) == 2
    doc = collection.find_one({"Name": "John Doe"})
    assert doc["Gender"] == "Male"
    assert doc["Date of Admission"].date() == pendulum.date(2023, 1, 15)
    assert isinstance(doc["Billing Amount"], float)

def test_schema_rejects_invalid_doc(mock_mongo_client):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)

    invalid_doc = {
        "Name": "Missing Blood Type",
        "Age": 45,
        "Gender": "Male",
        "Medical Condition": "Asthma",
        "Date of Admission": pendulum.now(),
        "Doctor": "Dr. Error",
        "Hospital": "Nowhere Hospital",
        "Billing Amount": 1234.56,
        "Room Number": 101,
        "Admission Type": "Emergency",
        "Discharge Date": pendulum.now(),
        "Medication": "Aspirin",
        "Test Results": "Normal"
    }

    with pytest.raises(OperationFailure):
        collection.insert_one(invalid_doc)

def test_batch_logging_and_processing(mock_mongo_client, test_csv, caplog):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)
    load_data(str(test_csv), collection, batch_size=1)

    assert "documents inserted" in caplog.text
    assert collection.count_documents({}) == 2

def test_malformed_csv_logs_error(mock_mongo_client, tmp_path, caplog):
    malformed_path = tmp_path / "bad.csv"
    malformed_path.write_text("Name,Age\nJohn,not_a_number")

    collection = get_collection(mock_mongo_client)
    create_schema(collection)

    load_data(str(malformed_path), collection)
    assert "Conversion error" in caplog.text
    assert "contains no valid docs" in caplog.text

def test_empty_csv(mock_mongo_client, tmp_path, caplog):
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("Name,Age,...,Test Results\n")

    collection = get_collection(mock_mongo_client)
    create_schema(collection)

    load_data(str(empty_csv), collection)
    assert "contains no valid docs" in caplog.text
    assert collection.count_documents({}) == 0

def test_partial_schema_doc(mock_mongo_client):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)

    doc = {
        "Name": "Extra Field",
        "Age": 60,
        "Gender": "Male",
        "Blood Type": "O+",
        "Medical Condition": "Cancer",
        "Date of Admission": pendulum.now(),
        "Doctor": "Dr. Extra",
        "Hospital": "Test Hospital",
        "Insurance Provider": "TestCare",
        "Billing Amount": 12345.0,
        "Room Number": 101,
        "Admission Type": "Emergency",
        "Discharge Date": pendulum.now(),
        "Medication": "Morphine",
        "Test Results": "Normal",
        "Extra Field": "Not in schema"
    }
    collection.insert_one(doc)
    assert collection.count_documents({}) == 1

def test_gender_capitalization(mock_mongo_client, tmp_path):
    df = pd.DataFrame({
        "Name": ["Case Test"],
        "Age": [25],
        "Gender": ["MaLE"],
        "Blood Type": ["A+"],
        "Medical Condition": ["None"],
        "Date of Admission": ["2024-01-01"],
        "Doctor": ["doc"],
        "Hospital": ["hospital"],
        "Insurance Provider": ["provider"],
        "Billing Amount": [100.0],
        "Room Number": [100],
        "Admission Type": ["urgent"],
        "Discharge Date": ["2024-01-05"],
        "Medication": ["med"],
        "Test Results": ["normal"]
    })
    csv_path = tmp_path / "case_test.csv"
    df.to_csv(csv_path, index=False)

    collection = get_collection(mock_mongo_client)
    create_schema(collection)
    load_data(str(csv_path), collection)

    doc = collection.find_one({"Name": "Case Test"})
    assert doc["Gender"] == "Male"
    assert doc["Admission Type"] == "Urgent"

def test_date_parsing(mock_mongo_client, test_csv):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)
    load_data(str(test_csv), collection)

    for doc in collection.find():
        assert hasattr(doc["Date of Admission"], "year")
        assert hasattr(doc["Discharge Date"], "year")

def test_data_integrity_against_schema(mock_mongo_client, test_csv):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)

    df = pd.read_csv(test_csv)
    expected_cols = [
        "Name", "Age", "Gender", "Blood Type", "Medical Condition",
        "Date of Admission", "Doctor", "Hospital", "Insurance Provider",
        "Billing Amount", "Room Number", "Admission Type", "Discharge Date",
        "Medication", "Test Results"
    ]
    for col in expected_cols:
        assert col in df.columns, f"Missing column: {col}"
        assert df[col].isnull().sum() == 0, f"Column {col} has missing values"

    load_data(str(test_csv), collection)
    doc = collection.find_one({"Name": "John Doe"})
    assert isinstance(doc["Age"], int)
    assert isinstance(doc["Gender"], str)
    assert isinstance(doc["Billing Amount"], float)
    assert isinstance(doc["Date of Admission"], pd.Timestamp) or hasattr(doc["Date of Admission"], "year")

    duplicates = collection.count_documents({
        "Name": "John Doe",
        "Date of Admission": doc["Date of Admission"]
    })
    assert duplicates == 1

def test_no_null_fields_after_migration(mock_mongo_client, test_csv):
    collection = get_collection(mock_mongo_client)
    create_schema(collection)
    load_data(str(test_csv), collection)

    doc = collection.find_one()
    for key in patient_schema["required"]:
        assert key in doc, f"Missing required field {key}"
        assert doc[key] is not None, f"Null value in field {key}"

def test_csv_and_mongo_count_match(mock_mongo_client, test_csv):
    df = pd.read_csv(test_csv)
    collection = get_collection(mock_mongo_client)
    create_schema(collection)
    load_data(str(test_csv), collection)

    assert collection.count_documents({}) == len(df)
