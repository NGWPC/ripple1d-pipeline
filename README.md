# Ripple1D Pipeline
 
Ripple1D Pipeline is a workflow that utilizes the [Ripple1d](https://github.com/Dewberry/ripple1d) to generate FIMs and rating curves. 
Compatible with ripple1d==0.7.0. Use repository tags to get older versions.

## Contents
- [Initialization/Pre Processing sripts](scripts/setup)
- [Ripple1d API Calls/Processing scripts](scripts/process)
- [Quality Control/Post processin scripts](scripts/qc)


## Dependencies
 - Ripple1d
 - Python version >=3.10 
 - Windows environment with Desktop Experience (GUI, not headless Windows)
 - HEC-RAS (v6.3.1)
 - Flows2fim Executable (if creating composite rasters)

### Getting Started

#### 1. **Checkout the Repo**
   - Clone this repository to your local machine.


#### 2. **Create a Virtual Environment**
   - Create a virtual environment in Python:
     - **Windows:**
     ```Powershell
     cd C:\venvs *OR* mkdir C:\venvs 
     python3 -m venv ripple1d_pipeline
     ```
     - **Linux**
     ```bash
     mkdir -p /venvs && cd /venvs
     python3 -m venv ripple1d_pipeline
     ```

#### 3. **Activate the Virtual Environment**
   - **Windows:**
     ```Powershell
     ripple1d_pipeline\Scripts\activate
     ```
   - **Mac/Linux:**
     ```bash
     source ripple1d_pipeline/bin/activate
     ```

#### 4. **Navigate to the Root of the Repo**
   - Use your terminal to navigate to the root folder of the repository:
   - **Windows:**
     ```Powershell
     cd d\ path\to\your\repo
     ```
   - **Mac/Linux:**
     ```bash
     cd path/to/your/repo
     ```

#### 5. **Install Requirements**
   - Install the necessary dependencies in your virtual environment:
     ```bash
     pip install -r requirements.txt
     ```

#### 6. **Set up and start Ripple1D Server** 
   - Create ripple1d virtual environment, activate, install ripple1d, start ripple1d. (Ripple1d server must be installed and ran on a windows machine, with HEC-Ras installed)
      ```Powershell
      cd d\ C:\venvs
      python3 -m venv ripple1d_<ripple1d version>
      cd ripple1d_<ripple1d version>
      .\Scripts\activate
      pip install ripple1d==<ripple1d version>
      ripple1d start --thread_count <number less than total available CPUs>
      ```

### Notes on Setup:
It is highly recommended to run all steps using the Windows Command Prompt Terminal Window Application, not the Windonws PowerShell application.

---

## **Running the Pipeline for A Collection**

### **Using batch_ripple_pipeline.py or ripple_pipeline.py**

### **Using Notebooks**

#### 1. **Update the Configuration File**
   - Modify the `config.py` file if needed to suit your collection-specific requirements (e.g., TERRAIN_SOURCE_URL, RIPPLE1D_API_URL, etc).

#### 2. **Access Notebooks**
   - **Option 1: Using VSCode**
     1. Open the notebook in VSCode.
     2. Point the kernel to the IPython kernel in your virtual environment.

   - **Option 2: Using Jupyter Lab**
     1. Start Jupyter Lab from your virtual environment:
        ```bash
        jupyter lab
        ```
     2. Open `localhost:8888` in your browser to access Jupyter Lab and open the notebooks.

#### 3. **Update Notebooks Parameters**
   - In the parameters cell of the notebooks, update the relevant paths and parameters to point to the working folder you created for the collection.

#### 4. **Execute and Export Notebooks as HTML**
   - Execute `setup_<collection_name>.ipynb` first and then `process_<collection_name>.ipynb` and finally `qc_<collection_name>.ipynb`
   - Once the notebooks are executed, export them as HTML files and move them into the working folder created for the collection.

#### 5. **Send for Quality Review**
   - After exporting, send the entire working folder for quality review.

## Outputs
Following outputs are produced for each batch that is processed:

`source_models`: Folder containing source models data, which were conflated and used as source for creating submodels for NWM reaches

`submodels`: Folders for extracted HEC-RAS submodels for NWM reaches that are used to create FIMs

`library`: Folder containing FIM rasters per reach and per flow and downstream boundary condition

`qc`: Folder containing data to evaluate quality of produced FIM library and rating curves

`error_report.xlsx`: Provide insight into the errors encountered during processing of each step

`ripple.gpkg`: Geopackage (SQLITE Database) containing records for reaches, models and rating curves

`start_reaches.csv`: Flows2FIM start file which can be used to create composite FIMs using Flows2FIM software

---