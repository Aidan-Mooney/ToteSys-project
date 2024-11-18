resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../requirements.depends.txt -t ${path.module}/../dependencies/python"
  }

  triggers = {
    dependencies = filemd5("${path.module}/../requirements.depends.txt")
  }
}

###############################################

data "archive_file" "dependencies"{
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../dependencies"
  output_path      = "${path.module}/../packages/layer/layer.zip"
}

data "archive_file" "utils"{
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../src/utils"
  output_path      = "${path.module}/../packages/utils/utils.zip"
}

################################################

resource "aws_lambda_layer_version" "dependencies" {
  layer_name          = "dependencies"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_object.dependencies_lambda_file.bucket
  s3_key              = aws_s3_object.dependencies_lambda_file.key
  depends_on          = [aws_s3_object.dependencies_lambda_file]
  source_code_hash    = data.archive_file.dependencies.output_base64sha256
}

resource "aws_lambda_layer_version" "ingest_utils" {
  layer_name          = "ingest_utils"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_object.ingest_utils_lambda_file.bucket
  s3_key              = aws_s3_object.ingest_utils_lambda_file.key
  depends_on          = [aws_s3_object.ingest_utils_lambda_file]
  source_code_hash    = data.archive_file.utils.output_base64sha256
}
