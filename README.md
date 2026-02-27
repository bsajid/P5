# Migration de données médicales vers MongoDB

## Contexte

Le client rencontre des problèmes de scalabilité avec sa gestion de données médicales. Ce projet migre un dataset CSV de patients vers **MongoDB**, une base NoSQL orientée documents qui offre une scalabilité horizontale native.

---

## Concepts clés MongoDB

### Document
L'unité de base de stockage. C'est un objet JSON (stocké en BSON) qui peut contenir des champs imbriqués. Équivalent d'une *ligne* en SQL.

json
{
  "_id": "ObjectId(...)",
  "personal_info": { "name": "Alice Johnson", "age": 45 },
  "medical_info":  { "condition": "Cancer", "test_results": "Inconclusive" }
}


### Collection
Regroupement de documents, analogue à une *table* SQL. Dans ce projet : collection **patients**.

### Base de données
Regroupe un ensemble de collections. Dans ce projet : base **medical_db**.

---

## Schéma de la base de données

Chaque ligne du CSV devient un document imbriqué par domaine métier. Ce schéma permet de lire le dossier complet d'un patient en **une seule requête**, sans jointure.


Document "patient"
│
├── personal_info
│   ├── name          String    "Alice Johnson"
│   ├── age           Int32     45
│   ├── gender        String    "Female"
│   └── blood_type    String    "AB-"
│
├── medical_info
│   ├── condition     String    "Cancer"
│   ├── medication    String    "Paracetamol"
│   └── test_results  String    "Inconclusive"
│
├── hospitalization
│   ├── date_of_admission  Date    ISODate("2024-01-03")
│   ├── discharge_date     Date    ISODate("2024-02-26")
│   ├── doctor             String  "Dr. Matthew Smith"
│   ├── hospital           String  "Sons and Miller"
│   ├── room_number        Int32   328
│   └── admission_type     String  "Urgent"
│
├── billing
│   ├── insurance_provider  String  "Aetna"
│   └── amount              Double  18856.28
│
└── metadata
    ├── imported_at  Date    ISODate("2024-...")
    └── source       String  "CSV migration"


---

## Structure du projet


P5/
├── data/
│   ├── medical_data.csv          # Dataset source
│   └── export_patients.json      # Export généré après migration
├── migrate.py                    # Script de migration
├── requirements.txt              # Dépendances Python
└── README.md                     # Ce fichier


---

## Prérequis

- Python 3.9+
- MongoDB installé et démarré en local
- MongoDB Compass (optionnel, pour visualiser les données)
- pip

---

## Installation des dépendances


powershell
pip install -r requirements.txt


## Lancement


python migrate.py


---

## Ce que fait le script

Le script migrate.py enchaîne les étapes suivantes à chaque exécution :

### 1 — Connexion MongoDB
Connexion à mongodb://localhost:27017/ et accès à la base medical_db, collection patients.

### 2 — Lecture du CSV
Lecture du fichier data/medical_data.csv avec pandas.

### 3 — Tests d'intégrité sur le CSV

| Contrôle | Détail |
|---|---|
| Colonnes présentes | Les 15 colonnes requises doivent exister |
| Doublons | Sur la clé Name + Date of Admission |
| Valeurs manquantes | Signalement colonne par colonne |
| Domaine Gender | Male / Female / Other |
| Domaine Blood Type | A+, A-, B+, B-, AB+, AB-, O+, O- |
| Domaine Admission Type | Elective / Emergency / Urgent |
| Domaine Test Results | Normal / Abnormal / Inconclusive |
| Plage Age | Entre 0 et 130 |
| Billing Amount | Doit être positif |
| Cohérence des dates | Discharge Date >= Date of Admission |

### 4 — Nettoyage et typage

| Colonne | Type MongoDB |
|---|---|
| Age, Room Number | Int32 (entier) |
| Billing Amount | Double (flottant) |
| Date of Admission, Discharge Date | Date (datetime) |
| Toutes les autres | String |

Les doublons sur Name + Date of Admission sont supprimés avant insertion.

### 5 — Insertion dans MongoDB
Insertion en masse avec insert_many. **Idempotente** : la collection est vidée avant chaque exécution pour éviter les doublons.

### 6 — Création des index

| Index | Champ indexé |
|---|---|
| idx_name | personal_info.name |
| idx_condition | medical_info.condition |
| idx_test_results | medical_info.test_results |
| idx_admission_date | hospitalization.date_of_admission |
| idx_hospital | hospitalization.hospital |
| idx_admission_type | hospitalization.admission_type |
| idx_insurance | billing.insurance_provider |

### 7 — Démonstration CRUD

python
# CREATE — insertion d'un document
collection.insert_one({ ... })

# READ — recherche et comptage
collection.find_one({ "personal_info.name": "Test Patient" })
collection.count_documents({ "medical_info.condition": "Diabetes" })

# UPDATE — mise à jour d'un champ
collection.update_one({ "_id": oid }, { "$set": { "personal_info.age": 31 } })

# DELETE — suppression d'un document
collection.delete_one({ "_id": oid })


### 8 — Tests d'intégrité dans MongoDB

Vérifications après migration :
- Nombre de documents en base = nombre de lignes CSV
- Aucun doublon sur name + date_of_admission
- Typage correct : age (int), amount (float), date_of_admission (datetime)
- Les 7 index sont présents
- Tous les patients du CSV sont bien en base

### 9 — Export JSON
La collection est exportée dans data/export_patients.json (sans _id, dates en ISO 8601).

Pour ré-importer :
powershell
mongoimport --uri "mongodb://localhost:27017/medical_db" --collection patients --file data/export_patients.json --jsonArray


---

## Visualiser les données avec MongoDB Compass

1. Ouvre **MongoDB Compass**
2. Connecte-toi sur mongodb://localhost:27017
3. Navigue vers medical_db → patients
4. Clique sur **Refresh** si la base n'apparaît pas

---

## Dépendances

| Package | Rôle |
|---|---|
| pymongo | Driver officiel MongoDB pour Python |
| pandas | Lecture et nettoyage du CSV |