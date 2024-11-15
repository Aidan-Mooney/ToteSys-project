resource "aws_s3_bucket" "ingest_bucket2" {
  bucket_prefix = var.ingest_bucket_prefix
  tags = {
    step = "ingest"
    purpose = "hold files containing updates to the database"
  }
}

# resource "aws_s3_bucket_versioning" "ingest_bucket_version" {
#   bucket = aws_s3_bucket.ingest_bucket2.id
#   versioning_configuration {
#     status = "Enabled"
#   }
# }

# resource "aws_s3_bucket_object_lock_configuration" "ingest_bucket_config" {
#   bucket = aws_s3_bucket.ingest_bucket2.id
#   rule {
#     default_retention {
#       mode = "GOVERNANCE"
#       days = 1
#     }
#   }
# }

resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = var.transform_bucket_prefix
  tags = {
    step = "transform"
    purpose = "hold the transformed data from the output of second lambda function"
  }
}

# resource "aws_s3_bucket_versioning" "transform_bucket_version" {
#   bucket = aws_s3_bucket.transform_bucket.id
#   versioning_configuration {
#     status = "Enabled"
#   }
# }

# resource "aws_s3_bucket_object_lock_configuration" "transform_bucket_config" {
#   bucket = aws_s3_bucket.transform_bucket.id
#   rule {
#     default_retention {
#       mode = "GOVERNANCE"
#       days = 1
#     }
#   }
# }