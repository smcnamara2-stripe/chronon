from __future__ import annotations

from ai.chronon.api.ttypes import GroupBy, Join, Source, Query, StagingQuery
from ai.chronon.repo.serializer import thrift_simple_json
from ai.chronon.utils import output_table_name, set_name, get_max_window_for_gb_in_days
from ai.chronon.repo import NOTEBOOKS_OUTPUT_NAMESPACE, NOTEBOOKS_LOG_FILE

from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from py4j.java_gateway import JavaObject, JVMView
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils
import os


class DatabricksExecutable:
    def __init__(self, spark_session: SparkSession) -> None:
        self._spark: SparkSession = spark_session
        self._jvm: JVMView = self._spark._jvm

        # Databricks Utils 
        self._dbutils = DBUtils(self._spark)

        # Constants Provider
        self._constants_provider: JavaObject = self._jvm.ai.chronon.spark.databricks.DatabricksConstantsNameProvider()
        self._jvm.ai.chronon.api.Constants.initConstantNameProvider(self._constants_provider)

        # Table Utils
        self._table_utils: JavaObject = self._jvm.ai.chronon.spark.databricks.DatabricksTableUtils(spark_session._jsparkSession)

        # Start date / end date defaults for analyze + validate operations
        self._default_start_date = (datetime.now() - timedelta(days=8)).strftime('%Y%m%d')
        self._default_end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    def _pretty_print_jvm_logs(self, start_stream_position:int, job_name: str) -> None:
        print("\n\n", "*" * 10, f" BEGIN LOGS FOR {job_name} ", "*" * 10)
        with open(NOTEBOOKS_LOG_FILE, "r") as file_handler:
                _ = file_handler.seek(start_stream_position)
                print(file_handler.read())
        print("*" * 10, f" END LOGS FOR {job_name} ", "*" * 10, "\n\n")

    def _get_databricks_user(self):
        user_email = self._dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        return user_email.split('@')[0].lower()
    
    def _set_metadata(self, obj):
        obj_type = type(obj)
        name_prefix = f"{self._get_databricks_user()}"
        
        # Note: name is what is used to determine the output table names
        # If the user is executing an object that is from zoolander, we will derive the name from the file path
        # If they defined the feature in cell we will require them to provide a name
        # In addition to this, we should always aim to prefix the name with the user and notebook name
        # This will prevent two users from overwriting each other's prototyping work
        if not obj.metaData.name:
            try:
                # Make an attempt to see if we can set the name attribute of the object
                # src here refers to the src folder in zoolander
                set_name(obj, obj_type, "src")
            except AttributeError:
                # If we can't determine the name using the garbage collector, that indicates that this feature was defined in cell where name should already have been provided.
                raise AttributeError("Please provide a name when defining group_bys/joins/staging_queries in a notebook cell. This can be done via the metaData.name attribute.")
        else:
            # Avoid adding the prefix multiple times
            obj.metaData.name = obj.metaData.name.replace(f"{name_prefix}_", "")

        # Additionally, we will want to set the name for any underlying joins that were used in JoinSources.
        # Note that we don't want to prefix the name for said underlying joins. We do that later in the event the user wants to execute the underlying join.
        # We have to do this so that the user can choose to read from the prod table for the underlying join if they choose to do so. 
        if obj_type == GroupBy:
            for s in obj.sources:
                if s.joinSource and not s.joinSource.join.metaData.name:
                    set_name(s.joinSource.join, Join, "src")
        elif obj_type == Join:
            for jp in obj.joinParts:
                for s in jp.groupBy.sources:
                    if s.joinSource and not s.joinSource.join.metaData.name:
                        set_name(s.joinSource.join, Join, "src")

        obj.metaData.outputNamespace = NOTEBOOKS_OUTPUT_NAMESPACE
        
        if obj_type == Join:
            for jp in obj.joinParts:
                jp.groupBy.metaData.outputNamespace = NOTEBOOKS_OUTPUT_NAMESPACE
        
        obj.metaData.name = f"{name_prefix}_{obj.metaData.name}"

        return obj

    def _get_query_with_updated_start_and_end_date(self, query: Query, start_date: str, end_date: str):
        """
        Get a new query with updated start and end date.
        """
        q = query
        q.startPartition = start_date
        q.endPartition = end_date
        return q
    
    def _get_source_with_updated_start_and_end_date(self, source: Source, start_date: str, end_date: str):
        """
        Get a new source with updated start and end date.
        """
        if source.events:
            source.events.query = self._get_query_with_updated_start_and_end_date(source.events.query, start_date, end_date)
        else:
            source.entities.query = self._get_query_with_updated_start_and_end_date(source.entities.query, start_date, end_date)
        return source
    
    def _get_sources_with_updated_start_and_end_date(self, sources: list, start_date: str, end_date: str):
        """
        Get a new list of sources with updated start and end date.
        """
        for source in sources:
            source = self._get_source_with_updated_start_and_end_date(source, start_date, end_date)
        return sources

    
    def _print_with_timestamp(self, message):
        """
        We want the output to go to the cell for the notebook instead of the logs so we use print instead of a logger.
        """
        current_utc_time = datetime.utcnow()
        time_str = current_utc_time.strftime('[%Y-%m-%d %H:%M:%S UTC]')
        print(f'{time_str} {message}')

    def _drop_table_if_exists(self, table_name: str):
        """
        Every time we run a staging query or group by backfill in Databricks we will drop the output table if it exists.
        We do this to avoid any issues with the table already existing.
        We don't need to do this for joins because we have archiving setup for that code flow. 
        """
        self._print_with_timestamp(f"Dropping table {table_name} if it exists.")
        self._spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    def _execute_join_sources_for_group_by(self, group_by:GroupBy, start_date:str, end_date:str, step_days:int) -> None: 
        join_sources = [s.joinSource for s in group_by.sources if s.joinSource and not s.joinSource.outputTableNameOverride]
        
        if not join_sources:
            return group_by
        
        max_window_for_gb_in_days = get_max_window_for_gb_in_days(group_by)
        shifted_start_date = (datetime.strptime(start_date, '%Y%m%d') - timedelta(days=max_window_for_gb_in_days)).strftime('%Y%m%d')

        self._print_with_timestamp(f"Executing {len(join_sources)} Join Source(s) for GroupBy {group_by.metaData.name} from {shifted_start_date} to {end_date} with step_days {step_days}\n\n")
        for js in join_sources:
            join_to_execute = js.join
            executable_join = DatabricksJoin(join_to_execute, self._spark)
            executable_join.run(shifted_start_date, end_date, step_days)
            output_table_name_for_js = output_table_name(join_to_execute, full_name=True)
            self._print_with_timestamp(f"When Join Source {join_to_execute.metaData.name} is rendered, Shepherd will read from {output_table_name_for_js}")

        
        self._print_with_timestamp(f"Join Source(s) for GroupBy {group_by.metaData.name} executed successfully.")

class DatabricksGroupBy(DatabricksExecutable):
    def __init__(self, group_by: GroupBy, spark_session: SparkSession) -> None:
        super().__init__(spark_session)
        self.group_by: GroupBy = self._set_metadata(group_by)


    def _get_group_by_to_execute(self, start_date: str) -> GroupBy:
        group_by_to_execute: GroupBy = self.group_by
        group_by_to_execute.backfillStartDate = start_date
        return group_by_to_execute
    
    def _get_java_group_by(self, group_by: GroupBy, end_date: str) -> JavaObject:
        """
        Converts our Python GroupBy object to a Java GroupBy object and handles raw S3 prefixes.
        """
        java_group_by: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.parseGroupBy(
            thrift_simple_json(group_by)
        )

        java_group_by_with_updated_s3_prefixes = self._jvm.ai.chronon.spark.S3Utils.readAndUpdateS3PrefixesForGroupBy(java_group_by, end_date, self._spark._jsparkSession)
        
        return java_group_by_with_updated_s3_prefixes

    def run(self, start_date:str, end_date: str, step_days: int = 30, skip_execution_of_underlying_join = False) -> DataFrame:
        """
        Performs a GroupBy Backfill operation.
        """

        self._print_with_timestamp(f"Executing GroupBy {self.group_by.metaData.name} from {start_date} to {end_date} with step_days {step_days}")
        self._print_with_timestamp(f"Skip Execution of Underlying Join Sources: {skip_execution_of_underlying_join}")

        if not skip_execution_of_underlying_join:
            self._execute_join_sources_for_group_by(self.group_by, start_date, end_date, step_days)

        group_by_to_execute: GroupBy = self._get_group_by_to_execute(start_date)
        group_by_output_table: str = output_table_name(group_by_to_execute, full_name=True)

        self._drop_table_if_exists(group_by_output_table)

        java_group_by: JavaObject = self._get_java_group_by(group_by_to_execute, end_date)

        starting_position_for_printing_jvm_logs = os.path.getsize(NOTEBOOKS_LOG_FILE)

        result_df_scala: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.runGroupBy(
            java_group_by,
            end_date,
            self._jvm.ai.chronon.spark.PySparkUtils.getIntOptional(str(step_days)),
            self._table_utils,
            self._constants_provider
        )

        self._pretty_print_jvm_logs(starting_position_for_printing_jvm_logs, f"Run GroupBy: {self.group_by.metaData.name}")

        self._print_with_timestamp(f"GroupBy {self.group_by.metaData.name} executed successfully and was written to iceberg.{group_by_output_table}")
        return DataFrame(result_df_scala, self._spark)

    def analyze(self, start_date:str = None, end_date: str = None, enable_hitter_analysis: bool = False):
        """
        Runs the analyzer on a Groupby.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """
        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date  

        self._print_with_timestamp(f"Analyzing GroupBy {self.group_by.metaData.name} from {start_date} to {end_date}")
        self._print_with_timestamp(f"Enable Hitter Analysis: {enable_hitter_analysis}")

        group_by_to_analyze: GroupBy = self._get_group_by_to_execute(start_date)

        java_group_by: JavaObject = self._get_java_group_by(group_by_to_analyze, end_date)

        starting_position_for_printing_jvm_logs = os.path.getsize(NOTEBOOKS_LOG_FILE)

        self._jvm.ai.chronon.spark.PySparkUtils.analyzeGroupBy(
            java_group_by,
            start_date, 
            end_date, 
            enable_hitter_analysis, 
            self._table_utils,
            self._constants_provider
        )

        self._pretty_print_jvm_logs(starting_position_for_printing_jvm_logs, f"Analyze GroupBy: {self.group_by.metaData.name}")

        self._print_with_timestamp(f"GroupBy {self.group_by.metaData.name} analyzed successfully.")
    

    def validate(self, start_date:str = None, end_date: str = None):
        """
        Runs the validator on a Groupby.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """
        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date

        self._print_with_timestamp(f"Validating GroupBy {self.group_by.metaData.name} from {start_date} to {end_date}")

        group_by_to_validate: GroupBy = self._get_group_by_to_execute(start_date)

        java_group_by: JavaObject = self._get_java_group_by(group_by_to_validate, end_date)

        starting_position_for_printing_jvm_logs = os.path.getsize(NOTEBOOKS_LOG_FILE)

        errors_list: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.validateGroupBy(
            java_group_by, 
            start_date, 
            end_date, 
            self._table_utils,
            self._constants_provider
        )

        self._pretty_print_jvm_logs(starting_position_for_printing_jvm_logs, f"Validate GroupBy: {self.group_by.metaData.name}")

        if errors_list.length() > 0:
            self._print_with_timestamp(f"Validation failed for GroupBy {self.group_by.metaData.name} with the following errors:")
            self._print_with_timestamp(errors_list)
        else:
            self._print_with_timestamp(f"Validation passed for GroupBy {self.group_by.metaData.name} .")

        self._print_with_timestamp(f"Validation for GroupBy {self.group_by.metaData.name} has completed.")
        

class DatabricksJoin(DatabricksExecutable):
    def __init__(self, join: Join, spark_session: SparkSession) -> None:
        super().__init__(spark_session)
        self.join: Join = self._set_metadata(join)
    
    def _get_join_to_execute(self, start_date: str, end_date: str) -> Join:
        join_to_execute: Join = self.join
        join_to_execute.left = self._get_source_with_updated_start_and_end_date(join_to_execute.left, start_date, end_date)
        return join_to_execute
    
    def _get_java_join(self, join: Join, end_date: str) -> JavaObject:
        """
        Converts our Python Join object to a Java Join object and handles raw S3 prefixes.
        """
        java_join: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.parseJoin(
            thrift_simple_json(join)
        )

        java_join_with_updated_s3_prefixes = self._jvm.ai.chronon.spark.S3Utils.readAndUpdateS3PrefixesForJoin(java_join, end_date, self._spark._jsparkSession)
        
        return java_join_with_updated_s3_prefixes

    def _execute_underlying_join_sources(self, start_date: str, end_date: str, step_days: int) -> None:
        for join_part in self.join.joinParts:
            self._execute_join_sources_for_group_by(join_part.groupBy, start_date, end_date, step_days)
        

    def run(self, start_date:str, end_date: str, step_days: int = 30, skip_first_hole: bool = False, sample_num_of_rows: int = None, skip_execution_of_underlying_join: bool = False) -> DataFrame:
        """
        Performs a Join Backfill operation.
        """
        self._print_with_timestamp(f"Executing Join {self.join.metaData.name} from {start_date} to {end_date} with step_days {step_days}")
        self._print_with_timestamp(f"Skip First Hole: {skip_first_hole}")
        self._print_with_timestamp(f"Sample Number of Rows: {sample_num_of_rows}")
        self._print_with_timestamp(f"Skip Execution of Underlying Join: {skip_execution_of_underlying_join}")

        if not skip_execution_of_underlying_join:
            self._execute_underlying_join_sources(start_date, end_date, step_days)


        join_to_execute: Join = self._get_join_to_execute(start_date, end_date)
        join_output_table: str = output_table_name(join_to_execute, full_name=True)

        java_join: JavaObject = self._get_java_join(join_to_execute, end_date)

        starting_position_for_printing_jvm_logs = os.path.getsize(NOTEBOOKS_LOG_FILE)

        result_df_scala = self._jvm.ai.chronon.spark.PySparkUtils.runJoin(
            java_join,
            end_date,
            self._jvm.ai.chronon.spark.PySparkUtils.getIntOptional(None if not step_days else str(step_days)),
            skip_first_hole,
            self._jvm.ai.chronon.spark.PySparkUtils.getIntOptional(None if not sample_num_of_rows else str(sample_num_of_rows)),
            self._table_utils,
            self._constants_provider
        )

        self._pretty_print_jvm_logs(starting_position_for_printing_jvm_logs, f"Run Join: {self.join.metaData.name}")

        self._print_with_timestamp(f"Join {self.join.metaData.name} executed successfully and was written to iceberg.{join_output_table}")
        return DataFrame(result_df_scala, self._spark)

    def analyze(self, start_date:str = None, end_date: str = None, enable_hitter_analysis: bool = False):
        """
        Runs the analyzer on a Join.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """

        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date

        self._print_with_timestamp(f"Analyzing Join {self.join.metaData.name} from {start_date} to {end_date}")
        self._print_with_timestamp(f"Enable Hitter Analysis: {enable_hitter_analysis}")

        join_to_analyze: Join = self._get_join_to_execute(start_date, end_date)

        java_join: JavaObject = self._get_java_join(join_to_analyze, end_date)

        starting_position_for_printing_jvm_logs = os.path.getsize(NOTEBOOKS_LOG_FILE)

        self._jvm.ai.chronon.spark.PySparkUtils.analyzeJoin(
            java_join,
            start_date, 
            end_date, 
            enable_hitter_analysis, 
            self._table_utils,
            self._constants_provider
        )

        self._pretty_print_jvm_logs(starting_position_for_printing_jvm_logs, f"Analyze Join: {self.join.metaData.name}")

        self._print_with_timestamp(f"Join {self.join.metaData.name} analyzed successfully.")
    
    def validate(self, start_date:str = None, end_date: str = None):
        """
        Runs the validator on a Join.
        If no start_date and end_date are provided, the default values of 8 days ago and 1 day ago are used.
        """

        start_date = start_date or self._default_start_date
        end_date = end_date or self._default_end_date

        self._print_with_timestamp(f"Validating Join {self.join.metaData.name} from {start_date} to {end_date}")

        join_to_validate: Join = self._get_join_to_execute(start_date, end_date)

        java_join: JavaObject = self._get_java_join(join_to_validate, end_date)

        starting_position_for_printing_jvm_logs = os.path.getsize(NOTEBOOKS_LOG_FILE)

        errors_list: JavaObject = self._jvm.ai.chronon.spark.PySparkUtils.validateJoin(
            java_join, 
            start_date, 
            end_date, 
            self._table_utils,
            self._constants_provider
        )

        self._pretty_print_jvm_logs(starting_position_for_printing_jvm_logs, f"Validate Join: {self.join.metaData.name}")

        if errors_list.length() > 0:
            self._print_with_timestamp(f"Validation failed for Join {self.join.metaData.name} with the following errors:")
            self._print_with_timestamp(errors_list)
        else:
            self._print_with_timestamp(f"Validation passed for Join {self.join.metaData.name} .")
        
        self._print_with_timestamp(f"Validation for Join {self.join.metaData.name} has completed.")