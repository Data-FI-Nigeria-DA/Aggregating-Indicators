# Aggregating Indicator Python Scripts

## Description

This repository contains automated scripts designed to aggregate indicators across different reports: **RADET**, **HTS**, **HTS_INDEX**, and **PMTCT_HTS**.

## 🚀 Getting Started

### 1\. Prerequisites

* **Python 3**
* **Google Colab**

### 2\. How to Run the Script

1.  **Download** the specific script you need.

2.  **Open** the file in any text editor (VS Code, Notepad, Sublime Text, etc.).

3.  **Edit the required variables** at the very top of the script (see the next section).

4.  **Save** the file.

5.  **Run the saved file in your Terminal or Command Prompt or on google colab.**


## 📝 Required Variable Changes

Each script is pre-configured with placeholder variables. You **MUST** update these two variables inside the Python file to point to your local machine's folders:

| Variable Name | Purpose | Example Value to Change |
| :--- | :--- | :--- |
| **`FOLDER_PATH`** | The **input directory** containing the raw data files that the script needs to check. | `"C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/FY26Q1_RADET"` |
| **`OUTPUT_BASE_DIR`** | The **output directory** where the final aggregate data will be saved. | `"C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/Project_Export_Quality_Check"` |
| **`Periods`** | The **Defining Period Section** where you will specify the period of analysis required. | `Start_of_quarter`, `End_of_quarter`, `six_months_ago` |
| **`viral_load_output_path`** | The **output directory** where viral load column and cleaned viral load column is extracted to for troubleshooting. | `"C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/Cleaned_Viral_Load_Values.xlsx"` |
| **`unique_cd4_output_path`** | The **output directory** where unique values in the Last CD4 column and cleaned Last CD4 column is extracted to for troubleshooting. | `"C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/Cleaned_CD4_Values.xlsx"` |
| **`hts_setting_output_path`** | The **output directory** where unique values in the Entry Point, Testing Setting  and Modality column are extracted to for troubleshooting. | `"C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/Extracted_HTS_setting.xlsx"` |

**⚠️ IMPORTANT:**
  
  * Keep the **quotes** around the file paths\!

-----

## 📁 Available Scripts

This repository contains the following aggregating indicator scripts:

| Script Filename | Indicators | Description |
| :--- | :--- | :--- |
| **`aggregate_data_radet.py`** | Treatment and Prevention | aggregates indicators specific to RADET report. |
| **`aggregate_data_hts.py`** | HTS | aggregates indicators specific to HIV Testing Services (HTS) report. |
| **`aggregate_data_pmtct_hts.py`** | PMTCT_HTS | aggregates indicators specific to Prevention of Mother-to-Child Transmission (PMTCT) report. |
| **`aggregate_data_hts_index.py`** | HTS_INDEX | aggregates indicators specific to HTS_INDEX report. |

-----

## ❓ Troubleshooting & Support

  * **Error running the script?** Double-check that your `FOLDER_PATH`, `OUTPUT_BASE_DIR` and other `output_paths' are correctly formatted and enclosed in quotes.

## Authors & Acknowledgement
-----
## 👥 Main Contributors

  * **[Arowolo Oluwabukola]** ([@Haddy-Oluwabukola](https://github.com/Haddy-Oluwabukola))
  

For further assistance, please contact the main contributor or open an **Issue** on this GitHub page.
