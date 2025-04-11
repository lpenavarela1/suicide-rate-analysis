import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SuicideRateMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains("YEAR")) return; // skip header

        String[] fields = line.split(",");
        if (fields.length < 13) return;

        String year = fields[7].trim();
        String age = fields[9].trim();
        String estimateStr = fields[11].trim();

        if (!age.equals("15–24")) return; // filter age group
        try {
            float estimate = Float.parseFloat(estimateStr);
            context.write(new Text(year), new FloatWritable(estimate));
        } catch (NumberFormatException e) {
            // skip if estimate is not a number
        }
    }
}
