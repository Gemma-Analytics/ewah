try: # load operators with dependencies installed, ignore others
    from ewah.operators.fx_operator import EWAHFXOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHFXOperator
try:
    from ewah.operators.google_analytics_operator import EWAHGAOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHGAOperator
try:
    from ewah.operators.mysql_operator import EWAHMySQLOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHMySQLOperator
try:
    from ewah.operators.oracle_operator import EWAHOracleSQLOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHOracleSQLOperator
try:
    from ewah.operators.postgres_operator import EWAHPostgresOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHPostgresOperator
try:
    from ewah.operators.s3_operator import EWAHS3Operator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHS3Operator
