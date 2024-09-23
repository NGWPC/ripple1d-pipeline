# Ripple1D Pipeline

Compatible with ripple1d==0.4.2
Use repository tags to get older versions

## Workflow

This guide provides instructions for setting up and running the Ripple1D pipeline.

### **First-Time Setup**

#### 1. **Checkout the Repo**
   - Clone the repository to your local machine.

#### 2. **Create a Virtual Environment**
   - Create a virtual environment in Python:
     ```bash
     python3 -m venv ripple1d_pipeline
     ```

#### 3. **Activate the Virtual Environment**
   - **Windows:**
     ```bash
     ripple1d_pipeline\Scripts\activate
     ```
   - **Mac/Linux:**
     ```bash
     source ripple1d_pipeline/bin/activate
     ```

#### 4. **Navigate to the Root of the Repo**
   - Use your terminal to navigate to the root folder of the repository:
     ```bash
     cd path/to/your/repo
     ```

#### 5. **Install Requirements**
   - Install the necessary dependencies in your virtual environment:
     ```bash
     pip install -r requirements.txt
     ```

---

### **Running the Pipeline for Each Batch**

#### 1. **Set Up a Workspace for Each Collection**
   - Create a working folder for the specific collection (for example `Z:\collections\EastFork_Trinity\)`. This folder will act as your workspace for the collection.

#### 2. **Duplicate and Rename Notebooks**
   - Duplicate both notebooks provided in the repo, and rename them to represent the current collection you are working on.

#### 3. **Update the Configuration File**
   - Modify the `config.py` file if needed to suit your collection-specific requirements (e.g., STAC_COLLECTION, TERRAIN_SOURCE_URL, RIPPLE1D_API_URL, etc).

#### 4. **Access Notebooks**
   - **Option 1: Using VSCode**
     1. Open the notebook in VSCode.
     2. Point the kernel to the IPython kernel in your virtual environment.

   - **Option 2: Using Jupyter Lab**
     1. Start Jupyter Lab from your virtual environment:
        ```bash
        jupyter lab
        ```
     2. Open `localhost:8888` in your browser to access Jupyter Lab and open the notebooks.

#### 5. **Update Notebooks Parameters**
   - In the parameters cell of the notebooks, update the relevant paths and parameters to point to the working folder you created for the collection.

#### 6. **Execute and Export Notebooks as HTML**
   - Execute `setup_<collection_name>.ipynb` first and then `process_<collection_name>.ipynb`
   - Once the notebooks are executed, export them as HTML files and move them into the working folder created for the collection.

#### 7. **Send for Quality Review**
   - After exporting, send the entire working folder for quality review.

## Outputs
Following outputs are produced for each batch that is processed:

`source_models`: Folder containing source models data, which were conflated and used as source for creating submodels for NWM reaches
`submodels`: Folders for extracted submodels for NWM reaches that are used to create FIMs
`library`: Folder containing FIM rasters per reach and per flow and downstream boundary condition
`qc`: Folder containing data to evaluate quality of produced FIM library and rating curves
`error_report.xlsx`: Provide insight into the errors encountered during processing of each step
`ripple.gpkg`: Geopackage (SQLITE Database) containing records for reaches, models and rating curves
`start_reaches.csv`: Flows2FIM start file which can be used to create composite FIMs using Flows2FIM software
---