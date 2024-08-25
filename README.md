# Ripple1D Pipeline

Currently works with ripple1d==0.4.1

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

#### 4. **Update Notebooks Parameters**
   - In the parameters cell of the notebooks, update the relevant paths and parameters to point to the working folder you created for the collection.

#### 5. **Run the Notebooks**
   - **Option 1: Using VSCode**
     1. Open the notebook in VSCode.
     2. Point the kernel to the IPython kernel in your virtual environment.

   - **Option 2: Using Jupyter Lab**
     1. Start Jupyter Lab from your virtual environment:
        ```bash
        jupyter lab
        ```
     2. Open `localhost:8888` in your browser to access Jupyter Lab and run the notebooks.

#### 6. **Export Notebooks as HTML**
   - Once the notebooks are executed, export them as HTML files and move them into the working folder created for the collection.

#### 7. **Send for Quality Review**
   - After exporting, send the entire working folder for quality review.

---