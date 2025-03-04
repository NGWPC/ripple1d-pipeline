job "test_secrets" {
  datacenters = ["dc1"]
  
  type="batch"
  
  parameterized {
    payload = "optional"
    meta_required = [
        "collection",
        "gitlab_un",
        "gitlab_pat",
        ]
  }

  constraint {
    attribute = "${node.class}"
    value     = "windows"
  }
  
  group "dispatch-pipeline" { 
    count=1
    
    task "process-ripple-collection" {
      driver = "raw_exec"

      config {
        command = "cmd.exe"
        args = [
          "/c",
          "local\\setup_and_pipeline.bat"
        ]
      }
      
      template {
        data = <<EOH
@echo off
echo Starting secret fetch at %DATE% %TIME%

REM Fetch the secret using AWS CLI
echo Fetching secret: nomad-oe-windows-client-20250228193251573500000001

aws secretsmanager get-secret-value --secret-id nomad-oe-windows-client-20250228193251573500000001 --query SecretString --output text > secrets\secret.json
if %ERRORLEVEL% NEQ 0 (
  echo Failed to retrieve secret
  exit /b 1
)

echo Secret retrieved successfully!
echo Contents of the secret:
type secrets\secret.json

REM Parse the JSON to .env format using PowerShell
powershell -Command "$secretJson = Get-Content -Raw -Path 'secrets\secret.json' | ConvertFrom-Json; $envContent = ''; $secretJson.PSObject.Properties | ForEach-Object { $envContent += \"$($_.Name)=$($_.Value)`n\" }; $envContent | Out-File -FilePath 'secrets\secret.env' -Encoding ascii"

echo Created .env file with secret values at %DATE% %TIME%
echo Secret fetch completed
EOH
        destination = "local/fetch-secret.bat"
      }

      template {
        data = <<EOF
echo "Get AWS Secrets" &&^
{{ env "NOMAD_TASK_DIR" }}\\fetch-secret.bat &&^
echo "clone repo from GitLab" &&^
git clone https://{{ env "NOMAD_META_gitlab_un" }}:{{ env "NOMAD_META_gitlab_pat" }}@gitlab.sh.nextgenwaterprediction.com/NGWPC/ripple1d-pipeline local\\ripple1d-pipeline &&^
echo "cd into {{ env "NOMAD_SECRETS_DIR" }}\\ripple1d-pipeline..." &&^
cd /d {{ env "NOMAD_TASK_DIR" }}\\ripple1d-pipeline &&^
echo "checkout AddNomadFunctionality...." &&^
git checkout AddNomadFunctionality &&^
echo "Copy env variables to .env file" &&^
echo F|xcopy /Y /F {{ env "NOMAD_SECRETS_DIR" }}\\secret.env {{ env "NOMAD_TASK_DIR" }}\\ripple1d-pipeline\\.env &&^
C:\venvs\ripple1d_pipeline\Scripts\activate &&^
pip install python-dotenv &&^
echo " Activated ripple1d_pipeline venv" &&^
echo "Submitting batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} ..." &&^
python batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} &&^
EXIT /b 0
EOF
        destination = "local/setup_and_pipeline.bat"
      }

      restart {
            mode = "fail"
            attempts = 1
      }

      resources {
        cpu    = 5000
        memory = 1084
      }
    }
  }
}
