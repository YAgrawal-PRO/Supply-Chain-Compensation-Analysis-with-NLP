# Supply-Chain-Compensation-Analysis-with-NLP
This project demonstrates the application of Natural Language Processing (NLP) and Machine Learning (ML) techniques, utilizing Python and Google Cloud Platform (GCP), to analyze unstructured text data (a thread of compensation replies) and extract structured insights on salaries, job roles, locations, and experience within the Supply Chain industry

<br>

**The project is divided into 5 parts:**
1. **Project Setup and Data Ingestion (GCP) 💻:** Establish the project environment and load the raw text data into a format suitable for processing.
    - **Setup:** Create a new project in Google Cloud Platform.
    - **Storage:** Use **Google Cloud Storage (GCS)** to host the raw text file of replies.
    - **Ingestion:** Write a Python script to read the data from the source file, which consists of a list of strings (the replies).

2. **Data Preprocessing and Feature Extraction (Python/NLP) 🧹:** Clean the text and extract key features (Salary, Role, Location, Experience) using Python and NLP techniques.

3. **Data Transformation and Storage (Python/GCP) 🏗️:** Convert the extracted features into a structured, tabular format and store it for analysis.

4. **Machine Learning/Statistical Analysis (Python/ML) 🧠:** Build a model to find relationships between variables.

5. **Insight Generation and Visualization (Python/GCP) 💡:** Visualize the findings and present the key insights derived from the data.