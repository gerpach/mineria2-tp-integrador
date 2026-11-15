"""Esquemas explícitos de Landing (nunca inferSchema). `value` se lee como texto porque llega
como número, como string numérico o null (perfil E1); el cast controlado se hace en Silver (Q4)."""
from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

CORRUPT = "_corrupt_record"


def _s(*fields):
    return StructType([StructField(n, t, True) for n, t in fields] + [StructField(CORRUPT, StringType(), True)])


CUSTOMERS_ORGS = _s(("org_id", StringType()), ("org_name", StringType()), ("industry", StringType()),
                    ("hq_region", StringType()), ("plan_tier", StringType()), ("is_enterprise", BooleanType()),
                    ("signup_date", DateType()), ("sales_rep", StringType()), ("lifecycle_stage", StringType()),
                    ("marketing_source", StringType()), ("nps_score", DoubleType()))

USERS = _s(("user_id", StringType()), ("org_id", StringType()), ("email", StringType()), ("role", StringType()),
           ("active", BooleanType()), ("created_at", DateType()), ("last_login", DateType()))

BILLING_MONTHLY = _s(("invoice_id", StringType()), ("org_id", StringType()), ("month", DateType()),
                     ("subtotal", DoubleType()), ("credits", DoubleType()), ("taxes", DoubleType()),
                     ("currency", StringType()), ("exchange_rate_to_usd", DoubleType()))

RESOURCES = _s(("resource_id", StringType()), ("org_id", StringType()), ("service", StringType()),
               ("region", StringType()), ("created_at", DateType()), ("state", StringType()),
               ("tags_json", StringType()))

# Maestros que van a Bronze batch: archivo -> (esquema, clave natural)
MASTERS = {
    "customers_orgs": (CUSTOMERS_ORGS, ["org_id"]),
    "users": (USERS, ["user_id"]),
    "billing_monthly": (BILLING_MONTHLY, ["invoice_id"]),
    "resources": (RESOURCES, ["resource_id"]),
}

# Unión de v1 y v2: en v1 carbon_kg y genai_tokens no existen y quedan NULL.
USAGE_EVENTS = _s(("event_id", StringType()), ("timestamp", TimestampType()), ("org_id", StringType()),
                  ("resource_id", StringType()), ("service", StringType()), ("region", StringType()),
                  ("metric", StringType()), ("value", StringType()), ("unit", StringType()),
                  ("cost_usd_increment", DoubleType()), ("schema_version", IntegerType()),
                  ("carbon_kg", DoubleType()), ("genai_tokens", LongType()))
