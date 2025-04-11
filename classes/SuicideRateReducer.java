import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class SuicideRateReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float total = 0;
        int count = 0;
        for (FloatWritable val : values) {
            total += val.get();
            count++;
        }
        float average = count == 0 ? 0 : total / count;
        context.write(key, new FloatWritable(average));
    }
}
