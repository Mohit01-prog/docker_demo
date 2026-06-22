from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

DATA_DIR = "data/customers.csv"
OUTPUT_DIR = "/tmp/customer_onboarding"


# Task A: Extract Customers
def extract_customers():

    if not os.path.exists(DATA_DIR):
        raise FileNotFoundError(f"{DATA_DIR} not found")

    customers = pd.read_csv(DATA_DIR)

    print(f"Extracted {len(customers)} customers.")

    return customers.to_dict("records")


# Task B: Validate Customers
def validate_customers(ti):

    customers = ti.xcom_pull(
        task_ids="extract_customers"
    )

    print("DEBUG customers:", customers)

    valid_customers = []

    for customer in customers:
        if (
            customer.get("name")
            and customer.get("email")
            and "@" in str(customer["email"])
        ):
            valid_customers.append(customer)

    print(f"{len(valid_customers)} customers validated successfully.")

    return valid_customers


# Task C: Load Customers
def load_customers(ti):

    customers = ti.xcom_pull(
        task_ids="validate_customers"
    )

    print("DEBUG valid_customers:", customers)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.DataFrame(customers)

    output_file = os.path.join(
        OUTPUT_DIR,
        "customers.csv"
    )

    df.to_csv(output_file, index=False)

    print(f"Customer data loaded to {output_file}")


# Task D: Send Welcome Emails
def send_welcome_email(ti):

    customers = ti.xcom_pull(
        task_ids="extract_customers"
    )

    print("DEBUG customers:", customers)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    email_log = os.path.join(
        OUTPUT_DIR,
        "emails_sent.txt"
    )

    with open(email_log, "w") as f:
        for customer in customers:

            message = (
                f"Welcome email sent to "
                f"{customer['name']} "
                f"({customer['email']})\n"
            )

            print(message.strip())
            f.write(message)

    print(f"Email log written to {email_log}")


with DAG(
    dag_id="customer_onboarding",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers
    )

    validate_task = PythonOperator(
        task_id="validate_customers",
        python_callable=validate_customers
    )

    load_task = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers
    )

    email_task = PythonOperator(
        task_id="send_welcome_email",
        python_callable=send_welcome_email
    )

    # Dependencies
    # extract_task >> validate_task >> load_task
    # extract_task >> email_task