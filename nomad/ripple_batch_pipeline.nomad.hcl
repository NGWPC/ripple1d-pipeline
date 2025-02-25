job "ripple_batch_pipeline" {
  datacenters = ["dc1"]
  
  type="batch"
  
  parameterized {
    payload = "optional"
    meta_required = ["collection"]
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
cd /d C:\Users\rob.pita\ripple1d-pipeline && ^
echo "cd into C:\Users\rob.pita\ripple1d-pipeline..." && ^
C:\venvs\ripple1d_pipeline\Scripts\activate && ^
echo "Activated ripple1d_pipeline vitrual environment..." && ^
python batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} && ^
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