job "old_ami_ripple_pipeline" {
  datacenters = ["dc1"]
  
  type="batch"
  
  parameterized {
    payload = "optional"
    meta_required = ["collection", "gitlab_un", "gitlab_pat"]
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
echo "clone repo from GitLab" &&^
git clone https://{{ env "NOMAD_META_gitlab_un" }}:{{ env "NOMAD_META_gitlab_pat" }}@gitlab.sh.nextgenwaterprediction.com/NGWPC/ripple1d-pipeline local\\ripple1d-pipeline &&^
echo "cd into local\\ripple1d-pipeline..." &&^
cd /d local\\ripple1d-pipeline &&^
echo "checkout ripple 0.7.0 commit sha...."
git checkout 6857a4d57414901e18a7483af7aba3e1b85adb2e &&^
icacls "C:\\venvs\\ripple1d_pipeline" /grant Everyone:(OI)(CI)RX &&^
echo "Changed file pemissions for C:\venvs\ripple1d-pipeline" &&^
icacls "{{ env "NOMAD_TASK_DIR" }}\ripple1d-pipeline" /grant Everyone:(OI)(CI)RX &&^
echo "Changed file pemissions for {{ env "NOMAD_TASK_DIR" }}\ripple1d-pipeline" &&^
C:\venvs\ripple1d_pipeline\Scripts\activate &&^
echo "Activated ripple1d_pipeline vitrual environment..." &&^
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