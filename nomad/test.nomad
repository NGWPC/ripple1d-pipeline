job "hostname-uploader2" {
  datacenters = ["dc1"]
  
  type="batch"
  
  parameterized {
    payload = "optional"
    meta_required = ["job_id"]
  }

  constraint {
    attribute = "${node.class}"
    value     = "windows"
  }
  
  group "windows-tasks" { 
    count=1
    
    task "upload-hostname" {
      driver = "raw_exec"
      
      config {
        command = "cmd.exe"
        args = [
          "/c",
          "(echo %COMPUTERNAME% > %COMPUTERNAME%.txt) && (aws s3 cp %COMPUTERNAME%.txt s3://fimc-data/ripple/test/)"
        ]
      }

      resources {
        cpu    = 5000 
        memory = 800 # In MB
      }

    }
  }
}