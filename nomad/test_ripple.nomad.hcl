job "test_riple_venv" {
  datacenters = ["dc1"]
  
  type="batch"
  
  parameterized {
    payload = "optional"
    meta_required = [
        "collection"
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
SET AWS_CONFIG_FILE=Z:\\shared\\aws_config &&^
echo "%AWS_PROFILE%" &&^
C:\venvs\ripple1d_pipeline\Scripts\python.exe Z:\ripple1d_pipeline\batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} &&^
exit /b 0
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
