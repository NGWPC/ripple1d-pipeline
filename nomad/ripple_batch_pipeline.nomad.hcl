job "ripple-batch-pipeline" {
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
@echo off
cd /d C:\ripple1d-pipeline &&^
echo "cd into C:\ripple1d-pipeline ..." &&^
C:\venvs\ripple1d_pipeline\Scripts\activate &&^
echo "Activated ripple1d_pipeline vitrual environment..." &&^
echo "Submitting batch_ripple_pipeline.py..." &&^
python batch_ripple_pipeline.py -l {{ env "NOMAD_META_collection" }} &&^
EXIT /b 0
EOF
        destination = "local/setup_and_pipeline.bat"
      }

      restart {
            mode = "fail"
            attempts = 0
      }

      resources {
        cpu    = 90000
        memory = 50000
      }

      logs {
        max_files     = 10
        max_file_size = 10
      }

      leader=true 
    }

    task "log-shipper"{
      driver = "raw_exec"
      
      config {
        command = "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe"
        args = [
          "-Command",
          "while ($true) { aws s3 cp $env:NOMAD_ALLOC_DIR/logs/ s3://fimc-data/ripple/100_pcnt_domain/nomad_logs/$env:NOMAD_META_collection --recursive --exclude \"log-shipper*\" ; Start-Sleep -Seconds 30 }"
        ]
      }

      resources {
        cpu    = 1000 
        memory = 800 # In MB
      }

      kill_timeout = "30s"
    }
  }
}

