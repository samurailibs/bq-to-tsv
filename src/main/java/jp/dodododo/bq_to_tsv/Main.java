package jp.dodododo.bq_to_tsv;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class Main {
    public static void main(String... args) {
        int status = new Main().process(args);
        System.exit(status);
    }

    public int process(String... args) {
        String sqlFile;
        String outTsvFile;
        try {
            sqlFile = args[0];
            outTsvFile = args[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Main sql_file output_tsv_file");
            return -1;
        }

        try {
            String query = readSqlFile(sqlFile);
            TableResult results = query(query);
            toTsv(results, outTsvFile);
        } catch (IOException | BigQueryException | InterruptedException e) {
            e.printStackTrace();
            return -1;
        }

        return 0;
    }


    protected String readSqlFile(String sqlFilePath) throws IOException {
        return FileUtils.readFileToString(new File(sqlFilePath), StandardCharsets.UTF_8);
    }

    protected TableResult query(String query) throws InterruptedException {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        return bigquery.query(queryConfig);


    }

    protected void toTsv(TableResult results, String outTsvFile) throws IOException {
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(outTsvFile), CSVFormat.MONGODB_TSV)) {
            Schema schema = results.getSchema();
            FieldList fields = schema.getFields();
            for (Field field : fields) {
                printer.print(field.getName());
            }
            printer.println();
            Iterable<FieldValueList> rows = results.iterateAll();
            for (FieldValueList row : rows) {
                for (FieldValue col : row) {
                    String val = getStringValue(col);
                    printer.print(val);
                }
                printer.println();
            }
        }
    }

    protected String getStringValue(FieldValue col) {
        if (col.isNull()) {
            return "null";
        }
        return col.getStringValue();
    }
}
