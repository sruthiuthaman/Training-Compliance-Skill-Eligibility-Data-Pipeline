# Databricks notebook source
# MAGIC %md
# MAGIC ## **QA Summit Training Overview**
# MAGIC
# MAGIC
# MAGIC
# MAGIC This notebook processes QA OCP Summit training transcript data to build a consolidated and compliant view of user trainings. It extracts, filters, and enriches training and curriculum data, maps them to skill blocks, and evaluates training, curriculum, and skill-level eligibility.
# MAGIC
# MAGIC The pipeline identifies overdue and upcoming trainings, determines whether users are eligible to perform tasks, and provides clear ineligibility reasons such as overdue or late completion. It also flags cases where a new training version is assigned but still within the allowed completion period.
# MAGIC
# MAGIC The final output is used for reporting and compliance tracking in Tableau dashboards.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # **INSTALL PACKAGES**

# COMMAND ----------

# %pip install Office365-REST-Python-Client==2.4.1 
# dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

from pyspark.sql.functions import (
    col, date_format, upper, when, row_number,
    regexp_replace, coalesce, lit
)


from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
import datetime  as d
from datetime import datetime
import pandas as pd
import io

# COMMAND ----------

# MAGIC %md
# MAGIC # **Connection to Sharepoint Excel file**

# COMMAND ----------

#Connect to sharepoint
u = 
p = 
username = u + '@its.jnj.com'
password = p
site_url='https://jnj.sharepoint.com/teams/JBVQuality2'
file_url='/teams/JBVQuality2/Shared Documents/GMP Training/GMP Training Curriculum matrix.xlsx'
sheet_name= 'Admin - Matrix raw data'

ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
file_response=File.open_binary(ctx, file_url)
#assert file_response.content[:2] == b'PK', "Not an Excel file returned"

df_Curr_Matrix = pd.read_excel(
    io.BytesIO(file_response.content),
    sheet_name=sheet_name,
    engine='openpyxl',
    dtype=str
)

#Chaging the PANDA DF to SPARK to avoid future data Error messages
spark_df_Curr_Matrix = spark.createDataFrame(df_Curr_Matrix)

spark_df_Curr_Matrix.printSchema()
spark_df_Curr_Matrix.count()

# display (spark_df_Curr_Matrix)
# display(spark_df_Curr_Matrix.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # **Connection to SQL Database**

# COMMAND ----------

def write_table_to_sql(df, database_prefix, table_name, mode='overwrite', env='PROD'):
    """
    Writes the specified table to the SQL Server database.
    
    Parameters:
        df (pdf): The pyspark data frame that will be loaded.
        database_prefix (str): The prefix used in the target table name.
        table_name (str): The original table name.
        mode (str): Write mode to be used (e.g., 'overwrite').
        env (str): Environment identifier (e.g., 'PROD').
    """
    
    # Establishing connection to db:
    # Get the hostname secret using the environment in the secret name.
    jdbcHostname = dbutils.secrets.get('jscan-dbscope-sqldb', f'jscan-dbsecret-datamodelingsqlsrv-{env.lower()}')
    jdbcDatabase = database_prefix
    jdbcPort = 1433 
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    
    # Retrieve database credentials from secrets (using environment)
    connectionProperties = {
        "user"     : dbutils.secrets.get('jscan-dbscope-sqldb', f'jscan-dbsecret-datamodelingsqluid-{env.lower()}'),
        "password" : dbutils.secrets.get('jscan-dbscope-sqldb', f'jscan-dbsecret-datamodelingsqlpwd-{env.lower()}'),
        "driver"   : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    # Construct target table name according to the pattern:
    # "dbo.<database_prefix>_QA_COMET_<table_name>_<env>"
    target_table = f"dbo.{database_prefix.upper()}_QA_{table_name.upper()}_{env.upper()}"
    
    # Write the dataframe (example: nc_capa_act_vp_resp) to SQL Server
    try:
        df.write.jdbc(jdbcUrl, target_table, mode=mode, properties=connectionProperties) 
        print(f'Table {target_table} successfully loaded into SQL')
    except Exception as e:
        print(f"An error occurred when loading table {target_table}: \n{e}")


# COMMAND ----------

# MAGIC %md
# MAGIC # **TRAINING FILTERS**

# COMMAND ----------

QA_SUMMIT_DATA_FILTERED = spark.sql("""
                                
                SELECT DISTINCT
                    smt.USER_WWID,
                    smt.USER_FULL_NAME, 
                    substring_index(
                        trim(substring_index(smt.USER_FULL_NAME, ',', -1)),
                        ' ',
                        1
                    ) AS USER_FIRST_NAME,
                    smt.USER_MANAGER_NAME,
                    smt.TRAINING_OBJECT_ID,
                    smt.TRAINING_TITLE,
                    smt.CURRICULUM_TITLE,
                    smt.TRAINING_VERSION,
                    smt.TRAINING_CUSTOM_COURSE_CODE,
                    smt.TRAINING_TYPE,
                    smt.GxP_TRAINING,
                    smt.TRAINING_STATUS,
                    smt.TRANSCRIPT_STATUS,
                    smt.CURRICULUM_TRANSCRIPT_STATUS,
                    smt.TRAINING_ACTIVE,
                    smt.TRAINING_PRODUCT_CODE,
                    smt.TRAINING_PROVIDER,
                    smt.IS_PART_OF_CURRICULUM_Q,
                    smt.IS_LATEST_TRAINING_VERSION_Q,
                    smt.TRANSCRIPT_REGISTRATION_NUMBER,
                    smt.TRANSCRIPT_ASSIGNED_DATE,
                    smt.TRANSCRIPT_DUE_DATE,
                    smt.TRANSCRIPT_COMPLETED_DATE,
                    smt.TRANSCRIPT_DOCUMENT_EFFECTIVE_DATE,
                    CASE 
                        WHEN smt.TRANSCRIPT_STATUS IS NULL THEN 'Exception / Other'
                        WHEN lower(smt.TRANSCRIPT_STATUS) LIKE '%past due%' THEN 'Pastdue'
                        WHEN smt.TRANSCRIPT_STATUS IN (
                            'Completed',
                            'Completed (Equivalent)',
                            'Approved',
                            'Exempt'
                        ) THEN 'Completed / Satisfied'
                        WHEN smt.TRANSCRIPT_STATUS IN (
                            'Registered',
                            'In Progress',
                            'Not Started',
                            'Pending Approval',
                            'Pending Grade',
                            'Pending Prerequisite',
                            'Pending Acknowledgement',
                            'Pending Completion Signature',
                            'Pending Completion Approval',
                            'Pending Evaluation',
                            'Pending Observer Completion',
                            'Pending Post-Work',
                            'Pending Pre-Work',
                            'Registration Pending',
                            'Waitlisted'
                        ) THEN 'Pending'
                        WHEN smt.TRANSCRIPT_STATUS IN (
                            'Cancelled',
                            'Denied',
                            'Failed',
                            'No Show',
                            'Incomplete',
                            'Withdrawn',
                            'Completion Approval Denied'
                        ) THEN 'Closed - Not Completed'
                        WHEN smt.TRANSCRIPT_STATUS IN (
                            'Exception Requested',
                            'Payment Refunded',
                            'Old Version',
                            'Registered / Not Available'
                        ) THEN 'Exception / Other'
                        ELSE 'Exception / Other'
                    END AS TRANSCRIPT_STATUS_GROUP    
                FROM cdl_ent_prd.prd_ent_silver_views.v01_qual_sum_transcript_curr smt 
                WHERE smt.TRANSCRIPT_IS_ASSIGNED_Q='True'
AND (smt.CURRICULUM_TRANSCRIPT_STATUS not in ('Old Version') OR SMT.CURRICULUM_TRANSCRIPT_STATUS is null)
and (smt.TRANSCRIPT_STATUS not in ('Old Version') or smt.TRANSCRIPT_STATUS is null)
AND smt.TRAINING_STATUS not in ('EXPR')
and smt.TRAINING_ACTIVE='Y'
and smt.USER_STATUS = 'Active'
and (smt.LATEST_CURRICULUM_VERSION_Q='True' or smt.LATEST_CURRICULUM_VERSION_Q is null)
and (smt.LATEST_TRANSCRIPT_VERSION_Q='True' or smt.LATEST_TRANSCRIPT_VERSION_Q is null)
and smt.REMOVED_FROM_ASSIGNMENT_Q is null
and (smt.MATERIAL_TYPE_ACTIVE!='False' or smt.TRAINING_TYPE='Test' or smt.TRAINING_TYPE='Online Class')
and (smt.REMOVED_FROM_TRANSCRIPT_Q !='True' OR smt.REMOVED_FROM_TRANSCRIPT_Q IS NULL) --NULL value extra added 
               AND (
                                    smt.USER_MANAGER_NAME = 'Beijer, Marieke'
                                    OR smt.INDIRECT_MANAGER_NAME = 'Beijer, Marieke'
                                    OR smt.USER_FULL_NAME        = 'Beijer, MariekeMR'
                                )
              ORDER BY smt.TRANSCRIPT_ASSIGNED_DATE DESC, smt.TRAINING_TITLE                                                                                                                                                                                               
""")

# Formatted date strings
for c in ["TRANSCRIPT_DUE_DATE", "TRANSCRIPT_COMPLETED_DATE", "TRANSCRIPT_ASSIGNED_DATE"]:
    QA_SUMMIT_DATA_FILTERED = QA_SUMMIT_DATA_FILTERED.withColumn(f"{c}_STR", date_format(col(c), "yyyy-MM-dd HH:mm"))

# QA_SUMMIT_DATA_FILTERED = QA_SUMMIT_DATA_FILTERED.cache()
# print(f"Rows found: {QA_SUMMIT_DATA_FILTERED.count()}")
# print(f"Final Rows found: {QA_SUMMIT_DATA_FILTERED.count()}")

display(QA_SUMMIT_DATA_FILTERED)
print(QA_SUMMIT_DATA_FILTERED.count())

#Load the filtered data to SQL server
# write_table_to_sql(QA_SUMMIT_DATA_FILTERED, "LEIDEN", "SUMMIT")
#----------------------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC # **CURRICULUM STATUS**

# COMMAND ----------

QA_SUMMIT_CURRICULUM_STATUS=spark.sql("""
 WITH open_trainings AS (
   SELECT DISTINCT
       smt.USER_WWID,
       smt.USER_FULL_NAME,
       smt.CURRICULUM_TITLE,
       COUNT(DISTINCT smt.TRAINING_TITLE) AS Num_Trainings
   FROM cdl_ent_prd.prd_ent_silver_views.v01_qual_sum_transcript_curr smt
   WHERE smt.TRANSCRIPT_IS_ASSIGNED_Q='True'
	AND (smt.CURRICULUM_TRANSCRIPT_STATUS not in ('Completed', 'Old Version','Completed (Equivalent)','Approved' ) OR SMT.CURRICULUM_TRANSCRIPT_STATUS is null)
	and (smt.TRANSCRIPT_STATUS not in ('Completed', 'Pending Observer Completion' ) or smt.TRANSCRIPT_STATUS is null)
	AND smt.TRAINING_STATUS not in ('EXPR')
	and smt.TRAINING_ACTIVE='Y'
	and (smt.LATEST_CURRICULUM_VERSION_Q='True' or smt.LATEST_CURRICULUM_VERSION_Q is null)
	and (smt.LATEST_TRANSCRIPT_VERSION_Q='True' or smt.LATEST_TRANSCRIPT_VERSION_Q is null)
	and smt.REMOVED_FROM_ASSIGNMENT_Q is null
    and  smt.USER_STATUS = 'Active'
	and (smt.MATERIAL_TYPE_ACTIVE!='False' or smt.TRAINING_TYPE='Test' or smt.TRAINING_TYPE='Online Class')
	and (smt.REMOVED_FROM_TRANSCRIPT_Q !='True' or smt.REMOVED_FROM_TRANSCRIPT_Q is null)
      AND (
                                    smt.USER_MANAGER_NAME = 'Beijer, Marieke'
                                    OR smt.INDIRECT_MANAGER_NAME = 'Beijer, Marieke'
                                    OR smt.USER_FULL_NAME        = 'Beijer, MariekeMR'
                                )
   GROUP BY
       smt.USER_WWID,
       smt.USER_FULL_NAME,
       smt.CURRICULUM_TITLE
)
SELECT DISTINCT
   smt.USER_WWID,
   smt.USER_FULL_NAME,
   smt.CURRICULUM_TITLE,
   CASE
       WHEN COALESCE(tr.Num_Trainings, 0) = 0 THEN 'Curriculum Completed'
       ELSE 'Curriculum not complete'
   END AS CURRICULUM_STATUS

FROM cdl_ent_prd.prd_ent_silver_views.v01_qual_sum_transcript_curr smt
LEFT JOIN  open_trainings tr ON smt.USER_WWID = tr.USER_WWID
    AND smt.CURRICULUM_TITLE = tr.CURRICULUM_TITLE
    WHERE smt.TRANSCRIPT_IS_ASSIGNED_Q='True'
	AND (smt.CURRICULUM_TRANSCRIPT_STATUS not in ('Old Version','Approved') OR SMT.CURRICULUM_TRANSCRIPT_STATUS is null)
	and (smt.TRANSCRIPT_STATUS not in ('Pending Observer Completion') or smt.TRANSCRIPT_STATUS is null)
	AND smt.TRAINING_STATUS not in ('EXPR')
	and smt.TRAINING_ACTIVE='Y'
    and  smt.USER_STATUS = 'Active'
	and (smt.LATEST_CURRICULUM_VERSION_Q='True' or smt.LATEST_CURRICULUM_VERSION_Q is null)
	and (smt.LATEST_TRANSCRIPT_VERSION_Q='True' or smt.LATEST_TRANSCRIPT_VERSION_Q is null)
	and smt.REMOVED_FROM_ASSIGNMENT_Q is null
	and (smt.MATERIAL_TYPE_ACTIVE!='False' or smt.TRAINING_TYPE='Test' or smt.TRAINING_TYPE='Online Class')
	and smt.REMOVED_FROM_TRANSCRIPT_Q !='True'
      AND (
                                    smt.USER_MANAGER_NAME = 'Beijer, Marieke'
                                    OR smt.INDIRECT_MANAGER_NAME = 'Beijer, Marieke'
                                    OR smt.USER_FULL_NAME        = 'Beijer, MariekeMR'
                                )
     order by CURRICULUM_STATUS desc                                                                      
                                      
                                      """)

# display(QA_SUMMIT_CURRICULUM_STATUS)
# print(QA_SUMMIT_CURRICULUM_STATUS.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Join the Filtered Data

# COMMAND ----------

QA_SUMMIT_MERGED = QA_SUMMIT_DATA_FILTERED.join(
    QA_SUMMIT_CURRICULUM_STATUS.select("USER_WWID","CURRICULUM_TITLE","CURRICULUM_STATUS"),
    on=["USER_WWID","CURRICULUM_TITLE"],
    how="left"
)

# display(QA_SUMMIT_MERGED)
# print(QA_SUMMIT_MERGED.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #  Task Matrix Filtering 

# COMMAND ----------

# from pyspark.sql import functions as F

df_matrix_clean = (
    spark_df_Curr_Matrix
        .select(
            F.col("HO"),
            F.col("Skill block ").alias("SKILL_BLOCK"),
            F.col("Curriculum Title").alias("CURRICULUM_TITLE")
        )
        .filter(F.col("CURRICULUM_TITLE").isNotNull())
        .filter(F.trim(F.col("CURRICULUM_TITLE")) != "")
        .filter(F.col("HO")=="PHARMA-HO-CBV-Quality-OCP")
        .withColumn("CURRICULUM_TITLE", F.upper(F.trim(F.col("CURRICULUM_TITLE"))))
        .withColumn("SKILL_BLOCK", F.upper(F.trim(F.col("SKILL_BLOCK"))))
        .dropDuplicates()
)
 

# display(df_matrix_clean)
# print(df_matrix_clean.count())


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


#Training‑level Yes/No flags

today = F.current_date()
next_7_days = F.date_add(today, 7)

df_with_training_flags = (
    QA_SUMMIT_MERGED
        .withColumn(
            "training_overdue",
            F.when(
                (F.col("TRANSCRIPT_DUE_DATE").isNotNull()) &
                (F.col("TRANSCRIPT_DUE_DATE") < today) &
                (F.col("TRANSCRIPT_STATUS_GROUP") != "Completed / Satisfied"),
                F.lit("Yes")
            ).otherwise(F.lit("No"))
        )
        .withColumn(
            "training_due_7_days",
            F.when(
                (F.col("TRANSCRIPT_DUE_DATE").isNotNull()) &
                (F.col("TRANSCRIPT_DUE_DATE").between(today, next_7_days)) &
                (F.col("TRANSCRIPT_STATUS_GROUP") != "Completed / Satisfied"),
                F.lit("Yes")
            ).otherwise(F.lit("No"))
        )
)
#Curriculum -Level Aggregation (Yes/No)
df_curriculum_flags = (
    df_with_training_flags
        .groupBy("USER_WWID", "CURRICULUM_TITLE")
        .agg(
            F.max(F.when(F.col("training_overdue") == "Yes", 1).otherwise(0))
                .alias("curriculum_overdue_num"),
            F.max(F.when(F.col("training_due_7_days") == "Yes", 1).otherwise(0))
                .alias("curriculum_due_7_days_num")
        )
        .withColumn(
            "curriculum_overdue",
            F.when(F.col("curriculum_overdue_num") == 1, "Yes").otherwise("No")
        )
        .withColumn(
            "curriculum_due_7_days",
            F.when(
                (F.col("curriculum_overdue_num") == 0) &
                (F.col("curriculum_due_7_days_num") == 1),
                "Yes"
            ).otherwise("No")
        )
        .drop("curriculum_overdue_num", "curriculum_due_7_days_num")
)
final_output_with_flags = (
    df_with_training_flags.alias("t")
        .join(
            df_curriculum_flags.alias("c"),
            on=["USER_WWID", "CURRICULUM_TITLE"],
            how="left"
        )
)

from pyspark.sql.functions import col, when, lit
#Transcript_Status_Group with Due 7 days Addition 
final_output_with_flags = (
    final_output_with_flags
    .withColumn(
        "TRANSCRIPT_STATUS_GROUP_TBL",
        when(col("training_overdue") == "Yes", lit("Pastdue"))
        .when(col("training_due_7_days") == "Yes", lit("Due_7_days"))
        .otherwise(col("TRANSCRIPT_STATUS_GROUP"))
    )
)

# display(final_output_with_flags)

# COMMAND ----------


summit_norm_full = (
    final_output_with_flags
        .withColumn(
            "CURRICULUM_TITLE_NORM",
            F.upper(F.trim(F.col("CURRICULUM_TITLE")))
        )
)

matrix_norm = (
    df_matrix_clean
        .withColumnRenamed("CURRICULUM_TITLE", "CURRICULUM_TITLE_NORM")
)

summit_with_tasks = (
    summit_norm_full.alias("s")
        .join(
            matrix_norm.alias("m"),
            on="CURRICULUM_TITLE_NORM",
            how="left"
        )
)

df = summit_with_tasks

# Checking new Training version is avilable and Timing compliance flags -----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = df.withColumn(
    "TRAINING_ELIGIBLE_Q",
    F.when(
        # Completed on time
        (F.col("TRANSCRIPT_STATUS_GROUP") == "Completed / Satisfied") &
        (
            F.col("TRANSCRIPT_DUE_DATE").isNull() |
            (F.col("TRANSCRIPT_COMPLETED_DATE") <= F.col("TRANSCRIPT_DUE_DATE"))
        ),
        1
    )
    .when(
        # Registered / Pending AND not overdue
        (F.col("TRANSCRIPT_STATUS_GROUP") == "Pending") &
        (
            F.col("TRANSCRIPT_DUE_DATE").isNull() |
            (F.col("TRANSCRIPT_DUE_DATE") >= F.current_date())
        ),
        1
    )
    # -----------------------------------#
    .when(
        # Registered / Pending and NOT overdue
        (F.col("training_overdue") == "No") &
        (F.col("TRANSCRIPT_STATUS_GROUP") != "Completed / Satisfied"),
        1
    )
# -----------------------------------------#
    .otherwise(
        # Overdue or completed late
        0
    )
)
# ------------------ Training‑level ineligible reason----------------------

df = df.withColumn(
    "TRAINING_INELIGIBLE_REASON",
    F.when(
        # Any non-completed training that is overdue
        (F.col("training_overdue") == "Yes") &
        (F.col("TRANSCRIPT_STATUS_GROUP") != "Completed / Satisfied"),
        "OVERDUE (NOT COMPLETED)"
    )
    .when(
        # Completed but after due date
        (F.col("TRANSCRIPT_STATUS_GROUP") == "Completed / Satisfied") &
        (F.col("TRANSCRIPT_COMPLETED_DATE").isNotNull()) &
        (F.col("TRANSCRIPT_DUE_DATE").isNotNull()) &
        (F.col("TRANSCRIPT_COMPLETED_DATE") > F.col("TRANSCRIPT_DUE_DATE")),
        "COMPLETED LATE"
    )
    .otherwise("OK")
)

#  -----------------------Curriculum‑level eligibility-------------------------
w_curr = Window.partitionBy("USER_WWID", "CURRICULUM_TITLE")

df = df.withColumn(
    "CURRICULUM_ELIGIBLE_Q",
    F.min(F.col("TRAINING_ELIGIBLE_Q")).over(w_curr)
)
 
# ------------------------skill level eligibility --------------------------------
w_skill = Window.partitionBy("USER_WWID", "SKILL_BLOCK")

df = df.withColumn(
    "SKILL_STATUS",
    F.when(
        F.min(F.col("CURRICULUM_ELIGIBLE_Q")).over(w_skill) == 1,
        "completed"
    ).otherwise("not completed")
)

df = df.withColumn(
    "TASK_ELIGIBILITY",
    F.when(F.col("SKILL_STATUS") == "completed", "ELIGIBLE")
     .otherwise("NOT ELIGIBLE")
)

# -----Skill level ineligibility reason -------------------------------

df = df.withColumn(
    "SKILL_INELIGIBLE_REASON_CODE",
    F.max(
        F.when(F.col("TRAINING_INELIGIBLE_REASON") == "OVERDUE (NOT COMPLETED)", 2)
         .when(F.col("TRAINING_INELIGIBLE_REASON") == "COMPLETED LATE", 1)
         .otherwise(0)
    ).over(w_skill)
)

df = df.withColumn(
    "SKILL_INELIGIBLE_REASON",
    F.when(F.col("SKILL_INELIGIBLE_REASON_CODE") == 2, "OVERDUE (NOT COMPLETED)")
     .when(F.col("SKILL_INELIGIBLE_REASON_CODE") == 1, "COMPLETED LATE")
     .otherwise("ELIGIBLE")
)

# -------------------------------New Version (Info only)------------------------------------------


df = df.withColumn(
    "NEW_VERSION_AVAILABLE_Q",
    F.when(
        # Latest version exists, not completed yet, but not overdue
        (F.col("TRANSCRIPT_STATUS_GROUP") == "Pending") &
        (
            F.col("TRANSCRIPT_DUE_DATE").isNull() |
            (F.col("TRANSCRIPT_DUE_DATE") >= F.current_date())
        ) &
        (F.col("training_overdue") == "No"), #----------------------#
        1
    ).otherwise(0)
)


df = df.withColumn(
    "NEW_VERSION_AVAILABLE",
    F.when(
        F.max(F.col("NEW_VERSION_AVAILABLE_Q")).over(w_skill) == 1,
        "Yes"
    ).otherwise("No")
)


# ------------------------------------------------------------------------
# -------------------------------------------------------------------------
final_output = df

# add date updated

final_output=final_output.withColumn("Date_Updated",F.lit(datetime.now()))

# display(final_output)

# COMMAND ----------

# MAGIC %md
# MAGIC #  **Write to SQL**

# COMMAND ----------

write_table_to_sql(final_output,"LEIDEN", "SUMMIT")