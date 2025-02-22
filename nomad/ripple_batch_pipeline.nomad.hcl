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
  
  group "windows-tasks" { 
    
    task "process_ripple_collection" {
      count=1
      driver = "raw_exec"
      
      config {
        command = "cmd.exe"
        args = [
          "/c",
          "batch_ripple_pipeline.py",
          "${NOMAD_META_collection}"
        ]
      }

      resources {
        cpu    = 5000 
        memory = 800 # In MB
      }

      
    }
  }
}