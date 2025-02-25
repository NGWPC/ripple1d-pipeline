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
aws_access_key_id={{ env "NOMAD_META_aws_access_key_id" }}
aws_secret_access_key={{ env "NOMAD_META_aws_secret_access_key" }}
region={{ env "NOMAD_META_aws_region" }}
EOF
        destination = "local\\ripple1d-pipeline\\.env"
        env = true 
      }

      template {
        data = <<EOF
echo "clone repo from GitLab" &&^
git clone https://{{ env "NOMAD_META_gitlab_un" }}:{{ env "NOMAD_META_gitlab_pat" }}@gitlab.sh.nextgenwaterprediction.com/NGWPC/ripple1d-pipeline local\\ripple1d-pipeline &&^
echo "cd into local\\ripple1d-pipeline..." &&^
cd /d local\\ripple1d-pipeline &&^
echo "checkout AddNomadFunctionality...."
git checkout AddNomadFunctionality &&^
python -m venv ripple1d_pipeline &&^
ripple1d_pipeline\Scripts\activate &&^
pip install -r requirements.txt &&^
echo "Activated ripple1d_pipeline vitrual environment and installed requirements..." &&^
python batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} &&^
echo "Submitted batch_ripple_pipeline.py..."
EOF
        destination = "local/setup_and_pipeline.bat"
      }

      resources {
        cpu    = 2000
        memory = 1084
      }
    }
  }
}