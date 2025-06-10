resource "aws_sns_topic" "extract_lambda_alerts" {
    name = "extract-lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "extract_error_emails_subscription" {
  topic_arn = aws_sns_topic.extract_lambda_alerts.arn
  protocol  = "email"
  endpoint  = "fscifa@googlegroups.com"
}

resource "aws_sns_topic" "transform_lambda_alerts" {
    name = "transform-lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "transform_error_emails_subscription" {
  topic_arn = aws_sns_topic.transform_lambda_alerts.arn
  protocol  = "email"
  endpoint  = "fscifa@googlegroups.com"
}

resource "aws_sns_topic" "load_lambda_alerts" {
    name = "load-lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "load_error_emails_subscription" {
  topic_arn = aws_sns_topic.load_lambda_alerts.arn
  protocol  = "email"
  endpoint  = "fscifa@googlegroups.com"
}


resource "aws_cloudwatch_metric_alarm" "extract_lambda_alarm" {
  alarm_name                = "extract-lambda-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Alarm for when extract lambda fails"
  alarm_actions = [aws_sns_topic.extract_lambda_alerts.arn]
  dimensions = {
    FunctionName = var.extract_lambda
  }
}


resource "aws_cloudwatch_metric_alarm" "lambda_transform_alarm" {
  alarm_name                = "transform-lambda-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Alarm for when transform lambda fails"
  alarm_actions = [aws_sns_topic.transform_lambda_alerts.arn]
  dimensions = {
    FunctionName = var.transform_lambda
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_load_alarm" {
  alarm_name                = "load-lambda-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Alarm for when load lambda fails"
  alarm_actions = [aws_sns_topic.load_lambda_alerts.arn]
  dimensions = {
    FunctionName = var.load_lambda
  }
}

