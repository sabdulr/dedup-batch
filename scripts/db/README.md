1. Check the Spring Batch metadata tables

The real failure is often stored in the step execution context / failure exception metadata.

Run this against your H2 batch metadata DB:

java -cp h2*.jar org.h2.tools.Shell \
  -url jdbc:h2:file:./work/batch-meta \
  -user sa

Then query:

SELECT STEP_EXECUTION_ID, STEP_NAME, STATUS, EXIT_CODE, READ_COUNT, WRITE_COUNT, FILTER_COUNT
FROM BATCH_STEP_EXECUTION
WHERE JOB_EXECUTION_ID = 545
ORDER BY STEP_EXECUTION_ID;

You are looking for the worker step with STATUS='FAILED'.

Then:

SELECT STEP_EXECUTION_ID, SHORT_CONTEXT
FROM BATCH_STEP_EXECUTION_CONTEXT
WHERE STEP_EXECUTION_ID = <failed_step_execution_id>;

And also:

SELECT JOB_EXECUTION_ID, STATUS, EXIT_CODE, EXIT_MESSAGE
FROM BATCH_JOB_EXECUTION
WHERE JOB_EXECUTION_ID = 545;


