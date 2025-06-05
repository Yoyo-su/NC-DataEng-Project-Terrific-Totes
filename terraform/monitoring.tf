resource "aws_sns_topic" "lambda_alerts" {
    name = "lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "error_emails_subscription" {
  topic_arn = aws_sns_topic.lambda_alerts.arn
  protocol  = "email"
  endpoint  = "fscifa@googlegroups.com"
}


resource "aws_cloudwatch_metric_alarm" "extract_lambda_alarm" {
  alarm_name                = "extract-lambda-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda"
  period                    = 900
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Alarm for when extract lambda fails"
  alarm_actions = [aws_sns_topic.lambda_alerts.arn]
  dimensions = {
    FunctionName = var.extract_lambda
  }
}


resource "aws_cloudwatch_metric_alarm" "transform_lambda_alarm" {
  alarm_name                = "transform-lambda-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda"
  period                    = 900
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Alarm for when transform lambda fails"
  alarm_actions = [aws_sns_topic.lambda_alerts.arn]
  dimensions = {
    FunctionName = var.transform_lambda
  }
}