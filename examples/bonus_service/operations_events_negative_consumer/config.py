"""Public portfolio configuration.

This file intentionally contains placeholders instead of real infrastructure
addresses, credentials, topic names, or test subscriber data.
"""

import os


environment = os.environ.get("PORTFOLIO_ENVIRONMENT", "local")

service_base_url = os.environ.get("SERVICE_BASE_URL", "https://example.test/api")
mock_service_url = os.environ.get("MOCK_SERVICE_URL", "https://mock.example.test")

db_host = os.environ.get("DB_HOST", "localhost")
db_port = int(os.environ.get("DB_PORT", "5432"))
db_user = os.environ.get("DB_USER", "portfolio_user")
db_password = os.environ.get("DB_PASSWORD", "portfolio_password")

redis_host = os.environ.get("REDIS_HOST", "localhost")
redis_password = os.environ.get("REDIS_PASSWORD", "portfolio_password")

sasl_username = os.environ.get("KAFKA_USERNAME", "portfolio_user")
sasl_password = os.environ.get("KAFKA_PASSWORD", "portfolio_password")
bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
external_bootstrap_servers = bootstrap_servers

account_data_topic = "Portfolio.Account.Data"
account_transactions_topic = "Portfolio.Account.Transactions"
bonus_operation_topic = "Portfolio.Bonus.Operation"
bonus_status_change_topic = "Portfolio.Bonus.StatusChange"
balance_end_of_day_topic = "Portfolio.Bonus.BalanceEndOfDay"
operation_events_topic = "Portfolio.Operations.Events"
operation_cancel_topic = "Portfolio.Operations.Cancel"
payment_result_topic = "Portfolio.Payments.Result"

# Backward-compatible aliases used by the copied examples.
accounts_data_for_portfolio_topic = account_data_topic
accounts_data_topic = account_data_topic
accounts_topic = "Portfolio.Accounts"
accounts_set_phone_retries_topic = "Portfolio.Accounts.SetPhoneRetries"
accounts_transactions_topic = account_transactions_topic
payment_provider_account_events_topic = "Portfolio.PaymentProvider.AccountEvents"
portfolio_payments_result_topic = payment_result_topic
portfolio_operations_events_topic = operation_events_topic
portfolio_operations_cancel_topic = operation_cancel_topic
portfolio_balance_end_of_day_topic = balance_end_of_day_topic
external_accounts_topic = "Portfolio.External.Accounts"

contract_msisdns = [
    "70000000000",
    "70000000000",
    "70000000000",
]

allure_url = "https://allure.example.test"
allure_testops_endpoint = "https://testops.example.test"
allure_testops_token = os.environ.get("ALLURE_TESTOPS_TOKEN", "placeholder-token")

payment_provider_password = os.environ.get("PAYMENT_PROVIDER_PASSWORD", "placeholder-password")
k8s_host = "example.test"
