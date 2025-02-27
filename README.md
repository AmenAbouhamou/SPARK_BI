# 📊 Projet BI - Analyse et Visualisation des Données Yelp


### 📌 Objectif
Ce projet vise à explorer les concepts de l'informatique décisionnelle en réalisant un processus complet, de la construction d'un data warehouse à l'exploitation des données et la restitution des résultats.

---

## 📂 Contenu du Projet

### 1️⃣ **Analyse et Modélisation**
- Étude du jeu de données fourni (Yelp Dataset)
- Définition des objectifs et des besoins analytiques
- Conception du schéma du data warehouse (approche Kimball ou Inmon)

### 2️⃣ **Intégration des Données (ETL)**
- Extraction des données à partir de fichiers CSV, JSON et PostgreSQL
- Transformation et nettoyage avec **Apache Spark (Scala ou Java)**
- Chargement des données dans un **data warehouse Oracle**

### 3️⃣ **Interrogation et Analyse**
- Conception de cubes OLAP et requêtes analytiques
- Étude des performances et optimisation des requêtes

### 4️⃣ **Visualisation et Restitution**
- Création de dashboards interactifs avec **Metabase**
- Présentation des résultats sous forme synthétique

---

## 🔧 Technologies Recommandées
- **Base de données** : PostgreSQL, Oracle
- **ETL** : Apache Spark (Scala/Java)
- **Requêtage** : SQL (PL/SQL, PostgreSQL)
- **Visualisation** : Metabase

---

## 📜 Contraintes & Livrables
📎 **Rapport (PDF, 15 pages min.)**
- Analyse du sujet et justification des choix
- Schémas et requêtes d'analyse
- Tableaux de bord et conclusions
- Documentation technique

📎 **Code Source & Scripts**
- Archivage en `.tgz` ou `.zip`
- Dépôt sur **Plubel**

📎 **Présentation orale (15 min)**
- Restitution pour un public décisionnel
- Démonstration des outils et analyses

---

## 📎 Connexions aux Bases de Données
### PostgreSQL
- **Port** : 5432
- **Base** : `tpid2020` (schéma `yelp`)
- **Login** : `tpid`
- **Mot de passe** : `tpid`
- **Connexion** : `psql -U tpid tpid2020`

### Oracle
- **Port** : 1521
- **Base** : `enss2024`
- **Login** : Identifiant IEM
- **Mot de passe** : Identique à l’identifiant

---

## 📚 Références
- [Documentation Yelp Dataset](https://www.yelp.com/dataset/documentation/main)
- [Guide SQL pour Spark](https://spark.apache.org/docs/latest/sql-getting-started.html)
- [Metabase - Installation](https://www.metabase.com/start/oss/jar.html)

---

## 🏆 Évaluation
L’évaluation repose sur :
✔ La qualité du **rapport** et de la **documentation**
✔ La **pertinence des choix techniques** et des analyses
✔ La **clarté de la présentation orale**
✔ L'optimisation et la performance des requêtes
