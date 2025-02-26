job "old_ami_ripple_pipeline" {
  datacenters = ["dc1"]
  
  type="batch"
  
  parameterized {
    payload = "optional"
    meta_required = [
        "collection",
        "gitlab_un",
        "gitlab_pat",
        "AWS_PROFILE",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_region"
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
        data = <<EOF
AWS_PROFILE={{ env "NOMAD_META_AWS_PROFILE" }}
RIPPLE1D_API_URL=http://127.0.0.1
STAC_URL=https://stac2.dewberryanalytics.com
EOF
        destination = "secrets/.env"
        env = true 
      }
      
      template {
        data = <<EOH
[profile {{ env "NOMAD_META_AWS_PROFILE" }}]
aws_access_key_id = {{ env "NOMAD_META_aws_access_key_id" }}
aws_secret_access_key = {{ env "NOMAD_META_aws_secret_access_key" }}
region = {{ env "NOMAD_META_aws_region" }}
output = json
EOH
        destination = "secrets/aws.config"
      }

      template {
        data = <<EOF
echo "clone repo from GitLab" &&^
git clone https://{{ env "NOMAD_META_gitlab_un" }}:{{ env "NOMAD_META_gitlab_pat" }}@gitlab.sh.nextgenwaterprediction.com/NGWPC/ripple1d-pipeline local\\ripple1d-pipeline &&^
echo "replacing default aws config file location, in setting AWS_CONFIG_FILE env" &&^
SET AWS_CONFIG_FILE={{ env "NOMAD_SECRETS_DIR" }}\aws.config
echo "cd into local\\ripple1d-pipeline..." &&^
cd /d local\\ripple1d-pipeline &&^
git checkout AddNomadFunctionality &&^
echo "checkout AddNomadFunctionality...." &&^
robocopy {{ env "NOMAD_SECRETS_DIR" }} . .env EXIT /b 0 &&^
echo "copy {{ env "NOMAD_SECRETS_DIR" }}\\.env to .env...." &&^
sleep 5 &&^
python -m venv ripple1d_pipeline &&^
echo " Created new ripple1d_pipeline venv"
ripple1d_pipeline\Scripts\activate &&^
echo " Activated ripple1d_pipeline venv"
pip install -r requirements.txt &&^
echo "Activated ripple1d_pipeline vitrual environment and installed requirements..." &&^
echo "Submitting batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} ..."
python batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} &&^
EOF
        destination = "local/setup_and_pipeline.bat"
      }

      resources {
        cpu    = 5000
        memory = 1084
      }
    }
  }
}
