"""The event contract: the ONE definition the generator, the streaming job, and
the Postgres DDL all derive from or are checked against.
"""

from pyspark.sql.types import StringType, StructField, StructType

# Every field is StringType. Deliberate, not an oversight: CSV is text, and this
# schema's only job is getting the right VALUE into the right COLUMN by
# position. Casting to real types (decimal, int, timestamp) happens explicitly
# in transforms.py, where a value that fails to cast is compared
# against its pre-cast string and routed to quarantine with a reason -- not
# silently turned into a NULL the way a strictly-typed reader schema would.
#
# `nullable` here is documentation of intent, not an enforced read-time
# constraint -- Spark's CSV reader does not reject a row for violating it.
# The real enforcement is transforms.add_rejection_reason, which is
# why the required/optional split below matches REQUIRED_FIELDS exactly.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_time", StringType(), nullable=False),
    StructField("generated_at", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=False),
    StructField("price", StringType(), nullable=True),
    StructField("quantity", StringType(), nullable=True),
])

# The generator's CSV writer imports THIS for its header/column order -- never
# a hand-written list of its own. See the module docstring for why that's the
# whole point.
CSV_COLUMNS = [f.name for f in EVENT_SCHEMA.fields]

# Shared vocabulary so the generator's synthesis, transforms' validation,
# and the tests never drift on what a "valid" value looks like.
EVENT_TYPES = ["view", "add_to_cart", "purchase"]

CATEGORIES = [
    "electronics", "books", "clothing", "home", "toys",
    "sports", "beauty", "groceries", "automotive", "garden",
]

# Fields whose absence makes a row structurally unusable rather than merely
# business-rule-invalid -- checked first in transforms.add_rejection_reason,
# and mirrored by NOT NULL in sql/postgres_setup.sql's events table.
REQUIRED_FIELDS = ["event_id", "event_time", "generated_at", "user_id", "event_type"]

# Populated by Spark's PERMISSIVE parser with a structurally broken CSV line's
# original text (wrong field count). Confirmed directly: Spark fills
# in whatever fields it CAN still read positionally and only nulls the ones it
# couldn't -- it does not null the whole row -- while this column carries the
# raw line regardless. transforms.add_rejection_reason routes a
# non-null value here into quarantine ahead of every other check.
#
# Lives here, not in streaming.py (which builds the reader schema that uses
# it) or transforms.py (which reads it): both of those need this name, and
# streaming.py already sits upstream of transforms.py in the pipeline
# (streaming.py assembles source -> transforms -> sinks), so transforms.py
# importing it FROM streaming.py would be a circular import the moment
# streaming.py needs to call back into transforms.py to assemble that
# pipeline. schema.py has no internal dependents of its own, which is exactly
# what makes it the correct place for a constant two other modules both need.
CORRUPT_RECORD_COLUMN = "_corrupt_record"
