"""
migrate.py
----------
Migration des données médicales CSV -> MongoDB.
"""

import os
import json
import pandas as pd
from datetime import datetime
from pymongo import MongoClient

MONGO_URI   = os.getenv("MONGO_URI",        "mongodb://localhost:27017/")
DB_NAME     = os.getenv("MONGO_DB",         "medical_db")
COL_NAME    = os.getenv("MONGO_COLLECTION", "patients")
CSV_PATH    = os.getenv("CSV_PATH",         "data/medical_data.csv")
EXPORT_PATH = "data/export_patients.json"


# ==== 1. CONNEXION MONGODB ====
print("Connexion à MongoDB...")
client     = MongoClient(MONGO_URI)
db         = client[DB_NAME]
collection = db[COL_NAME]
print("Connecté !")

# ==== 2. LECTURE DU CSV ==== 
print("\nLecture du CSV...")
df = pd.read_csv(CSV_PATH)
print(f"{len(df)} lignes lues.")


# ==== 3. TESTS D'INTÉGRITÉ SUR LE CSV ==== 
print("\n--- Tests d'intégrité CSV ---")

# Colonnes attendues
colonnes_attendues = [
    "Name", "Age", "Gender", "Blood Type", "Medical Condition",
    "Date of Admission", "Doctor", "Hospital", "Insurance Provider",
    "Billing Amount", "Room Number", "Admission Type",
    "Discharge Date", "Medication", "Test Results",
]
for col in colonnes_attendues:
    if col not in df.columns:
        print(f"  [ERREUR] Colonne manquante : {col}")
    else:
        print(f"  [OK] Colonne '{col}' présente")

# Doublons
nb_doublons = df.duplicated(subset=["Name", "Date of Admission"]).sum()
if nb_doublons > 0:
    print(f"  [AVERT] {nb_doublons} doublon(s) trouvé(s)")
else:
    print("  [OK] Aucun doublon")

# Valeurs manquantes
for col in colonnes_attendues:
    nb_vides = df[col].isnull().sum()
    if nb_vides > 0:
        print(f"  [AVERT] '{col}' : {nb_vides} valeur(s) manquante(s)")

# Vérification Gender
valeurs_gender_valides = ["Male", "Female", "Other"]
for valeur in df["Gender"].unique():
    if valeur not in valeurs_gender_valides:
        print(f"  [AVERT] Gender inconnu : {valeur}")

# Vérification Blood Type
valeurs_blood_valides = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
for valeur in df["Blood Type"].unique():
    if valeur not in valeurs_blood_valides:
        print(f"  [AVERT] Blood Type inconnu : {valeur}")

# Vérification Admission Type
valeurs_admission_valides = ["Elective", "Emergency", "Urgent"]
for valeur in df["Admission Type"].unique():
    if valeur not in valeurs_admission_valides:
        print(f"  [AVERT] Admission Type inconnu : {valeur}")

# Vérification Test Results
valeurs_test_valides = ["Normal", "Abnormal", "Inconclusive"]
for valeur in df["Test Results"].unique():
    if valeur not in valeurs_test_valides:
        print(f"  [AVERT] Test Results inconnu : {valeur}")

# Vérification âge
for age in df["Age"]:
    if age < 0 or age > 130:
        print(f"  [AVERT] Age hors limites : {age}")
        break
else:
    print("  [OK] Tous les âges sont entre 0 et 130")

# Billing Amount positif
for montant in df["Billing Amount"]:
    if montant < 0:
        print(f"  [AVERT] Billing Amount négatif : {montant}")
        break
else:
    print("  [OK] Tous les montants sont positifs")

# Cohérence des dates
df["Date of Admission"] = pd.to_datetime(df["Date of Admission"], dayfirst=True)
df["Discharge Date"]    = pd.to_datetime(df["Discharge Date"],    dayfirst=True)
nb_dates_incoh = (df["Discharge Date"] < df["Date of Admission"]).sum()
if nb_dates_incoh > 0:
    print(f"  [AVERT] {nb_dates_incoh} ligne(s) avec Discharge < Admission")
else:
    print("  [OK] Les dates sont cohérentes")

# ==== 4. NETTOYAGE ET TYPAGE ==== 
print("\n--- Nettoyage ---")

# Supprimer les espaces dans les noms de colonnes et les valeurs texte
df.columns = df.columns.str.strip()
for col in df.select_dtypes("object").columns:
    df[col] = df[col].str.strip()

# Supprimer les doublons
nb_avant = len(df)
df = df.drop_duplicates(subset=["Name", "Date of Admission"])
nb_apres = len(df)
if nb_avant != nb_apres:
    print(f"  {nb_avant - nb_apres} doublon(s) supprimé(s)")

# Typage des colonnes
df["Age"]            = pd.to_numeric(df["Age"],            errors="coerce").astype("Int64")
df["Room Number"]    = pd.to_numeric(df["Room Number"],    errors="coerce").astype("Int64")
df["Billing Amount"] = pd.to_numeric(df["Billing Amount"], errors="coerce")
df["Date of Admission"] = pd.to_datetime(df["Date of Admission"], dayfirst=True)
df["Discharge Date"]    = pd.to_datetime(df["Discharge Date"],    dayfirst=True)

print(f"  {len(df)} lignes après nettoyage")


# ==== 5. INSERTION DANS MONGODB ==== 
print("\n--- Insertion ---")

# Vider la collection si elle existe déjà
nb_existants = collection.count_documents({})
if nb_existants > 0:
    collection.drop()
    print(f"  Collection vidée ({nb_existants} documents supprimés)")
    collection = db[COL_NAME]

# Convertir chaque ligne en document et insérer
documents = []
for index, ligne in df.iterrows():
    doc = {
        "personal_info": {
            "name":       str(ligne["Name"]) if pd.notna(ligne["Name"]) else None,
            "age":        int(ligne["Age"])  if pd.notna(ligne["Age"])  else None,
            "gender":     str(ligne["Gender"]),
            "blood_type": str(ligne["Blood Type"]),
        },
        "medical_info": {
            "condition":    str(ligne["Medical Condition"]),
            "medication":   str(ligne["Medication"]),
            "test_results": str(ligne["Test Results"]),
        },
        "hospitalization": {
            "date_of_admission": ligne["Date of Admission"].to_pydatetime(),
            "discharge_date":    ligne["Discharge Date"].to_pydatetime(),
            "doctor":            str(ligne["Doctor"]),
            "hospital":          str(ligne["Hospital"]),
            "room_number":       int(ligne["Room Number"]) if pd.notna(ligne["Room Number"]) else None,
            "admission_type":    str(ligne["Admission Type"]),
        },
        "billing": {
            "insurance_provider": str(ligne["Insurance Provider"]),
            "amount":             float(ligne["Billing Amount"]) if pd.notna(ligne["Billing Amount"]) else None,
        },
        "metadata": {
            "imported_at": datetime.utcnow(),
            "source":      "CSV migration",
        },
    }
    documents.append(doc)

collection.insert_many(documents)
print(f"  {len(documents)} documents insérés")


# ==== 6. CRÉATION DES INDEX ==== 
print("\n--- Création des index ---")

collection.create_index("personal_info.name",                name="idx_name")
collection.create_index("medical_info.condition",            name="idx_condition")
collection.create_index("medical_info.test_results",         name="idx_test_results")
collection.create_index("hospitalization.date_of_admission", name="idx_admission_date")
collection.create_index("hospitalization.hospital",          name="idx_hospital")
collection.create_index("hospitalization.admission_type",    name="idx_admission_type")
collection.create_index("billing.insurance_provider",        name="idx_insurance")

print("  Index créés")


# ==== 7. DÉMONSTRATION CRUD ==== 
print("\n--- Démonstration CRUD ---")

# CREATE : on insère un patient de test
patient_test = {
    "personal_info":   {"name": "Test Patient", "age": 30, "gender": "Male", "blood_type": "O+"},
    "medical_info":    {"condition": "Test", "medication": "Placebo", "test_results": "Normal"},
    "hospitalization": {
        "date_of_admission": datetime(2024, 1, 1),
        "discharge_date":    datetime(2024, 1, 3),
        "doctor": "Dr. Test", "hospital": "Test Hospital",
        "room_number": 0, "admission_type": "Elective",
    },
    "billing":  {"insurance_provider": "None", "amount": 0.0},
    "metadata": {"imported_at": datetime.utcnow(), "source": "demo"},
}
resultat_insert = collection.insert_one(patient_test)
print(f"  CREATE : document inséré avec _id = {resultat_insert.inserted_id}")

# READ : on le retrouve
doc_trouve = collection.find_one({"_id": resultat_insert.inserted_id})
print(f"  READ   : trouvé → {doc_trouve['personal_info']['name']}")

nb_diabete = collection.count_documents({"medical_info.condition": "Diabetes"})
print(f"  READ   : {nb_diabete} patient(s) avec 'Diabetes'")

# UPDATE : on change l'âge
collection.update_one(
    {"_id": resultat_insert.inserted_id},
    {"$set": {"personal_info.age": 31}}
)
doc_maj = collection.find_one({"_id": resultat_insert.inserted_id})
print(f"  UPDATE : âge mis à jour → {doc_maj['personal_info']['age']}")

# DELETE : on supprime le patient de test
collection.delete_one({"_id": resultat_insert.inserted_id})
doc_supprime = collection.find_one({"_id": resultat_insert.inserted_id})
print(f"  DELETE : supprimé → {doc_supprime is None}")


# ==== 8. TESTS D'INTÉGRITÉ DANS MONGODB ==== 
print("\n--- Tests d'intégrité MongoDB ---")

# Nombre de documents
nb_docs = collection.count_documents({})
if nb_docs == len(df):
    print(f"  [OK] {nb_docs} documents en base = {len(df)} lignes CSV")
else:
    print(f"  [AVERT] {nb_docs} documents en base ≠ {len(df)} lignes CSV")

# Doublons en base
pipeline_doublons = [
    {"$group": {
        "_id": {
            "name": "$personal_info.name",
            "date": "$hospitalization.date_of_admission"
        },
        "nb": {"$sum": 1}
    }},
    {"$match": {"nb": {"$gt": 1}}}
]
doublons_mongo = list(collection.aggregate(pipeline_doublons))
if doublons_mongo:
    print(f"  [AVERT] {len(doublons_mongo)} doublon(s) en base")
else:
    print("  [OK] Aucun doublon en base")

# Vérification des types sur un exemple
exemple = collection.find_one({})
if exemple:
    age_val  = exemple["personal_info"]["age"]
    mont_val = exemple["billing"]["amount"]
    date_val = exemple["hospitalization"]["date_of_admission"]
    print(f"  [OK] age         → {type(age_val).__name__}")
    print(f"  [OK] amount      → {type(mont_val).__name__}")
    print(f"  [OK] date        → {type(date_val).__name__}")

# Vérification des index
index_presents  = set(collection.index_information().keys())
index_attendus  = ["idx_name", "idx_condition", "idx_test_results",
                   "idx_admission_date", "idx_hospital",
                   "idx_admission_type", "idx_insurance"]
for idx in index_attendus:
    if idx in index_presents:
        print(f"  [OK] Index '{idx}' présent")
    else:
        print(f"  [AVERT] Index '{idx}' ABSENT")

# Tous les noms du CSV sont en base
noms_csv   = set(df["Name"].str.strip().tolist())
noms_mongo = {d["personal_info"]["name"] for d in collection.find({}, {"personal_info.name": 1, "_id": 0})}
noms_manquants = noms_csv - noms_mongo
if noms_manquants:
    print(f"  [AVERT] Noms absents en base : {noms_manquants}")
else:
    print(f"  [OK] Les {len(noms_csv)} patients du CSV sont tous en base")

# ==== 9. EXPORT JSON ==== 
print("\n--- Export JSON ---")

tous_les_docs = list(collection.find({}, {"_id": 0}))

# Les datetime ne sont pas sérialisables nativement, on les convertit en string
def convertir_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type non sérialisable : {type(obj)}")

with open(EXPORT_PATH, "w", encoding="utf-8") as fichier:
    json.dump(tous_les_docs, fichier, ensure_ascii=False, indent=2, default=convertir_datetime)

print(f"  Export terminé → {EXPORT_PATH} ({len(tous_les_docs)} documents)")

print("\n=== MIGRATION TERMINÉE ===")
print(f"  Documents en base : {collection.count_documents({})}")
print(f"  Base              : {DB_NAME}.{COL_NAME}")

client.close()