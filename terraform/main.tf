provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Pub/Sub Topic
resource "google_pubsub_topic" "flights-topic" {
  name = "flights-topic"
}

# 2. Subscription Topic
resource "google_pubsub_subscription" "flights-sub" {
  name  = "flights-sub"
  topic = google_pubsub_topic.flights-topic.name

  message_retention_duration = "604800s"
  ack_deadline_seconds = 20
  retain_acked_messages = false
  # expiration_policy: here we set the TTL to 0 to prevent automatic deletion of the subscription, allowing it to persist indefinitely until we choose to delete it manually. 
    expiration_policy { }
  #expiration_policy {
  #  ttl = "" 
  #}
}

# 2. temporary storage bucket for Dataflow
resource "google_storage_bucket" "dataflow_temp_bucket" {
  name          = "${var.project_id}-dataflow-temp"
  location      = var.region
  force_destroy = true 
  storage_class = "STANDARD"
}

# 3.PostgreSQL Database 
resource "google_sql_database_instance" "postgres_instance" {
  name             = "flight-db-instance"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = "db-f1-micro" 
    ip_configuration {
      ipv4_enabled = true
     
    }
    location_preference {
      zone = "${var.region}-a"
    }
    backup_configuration {
      enabled = false # Disable automatic backups to save costs
    }
  }
  deletion_protection = false 
}

resource "google_sql_database" "flight_db" {
  name     = "flight_data"
  instance = google_sql_database_instance.postgres_instance.name
}

 