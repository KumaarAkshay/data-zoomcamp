provider "google" {
  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region
}

# Google Storage Bucket
resource "google_storage_bucket" "test_bucket" {
  name          = "test_bucket_from_terraform"
  location      = var.region
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }
}

# Google Bigquery Dataset
resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# Google Compute Instance
resource "google_compute_instance" "test_instance" {
  name         = "zoom-vm"
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = var.image
      size  = 15
      type  = "pd-balanced"
    }
    auto_delete = true
  }

  network_interface {
    network = "default"

    # Adding external IP configuration
    access_config {
      # This ensures the instance gets an external IP
    }
  }

  tags = ["http-server"]
}

# Output the VM's external IP
output "vm_external_ip" {
  value = google_compute_instance.test_instance.network_interface[0].access_config[0].nat_ip
}
